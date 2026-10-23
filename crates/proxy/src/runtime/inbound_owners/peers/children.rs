//! One cold resource per actual peer; the existing peer permit includes retirement.
mod bound_datagram;
pub(crate) use bound_datagram::{PeerBoundDatagramAuthority, PeerBoundDatagramRegistration};
mod close;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "explicit single-target grant fixture; live multi-target peers use the bound provider"
    )
)]
mod datagram;
use super::*;
use crate::component::instance::{retained, SharedError};
use tokio::sync::Mutex as AsyncMutex;
use zero_core::component::{AssociationId, RoleInstanceId};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynResourceLifecycle, TaskFailure},
    ChannelError, CloseReason, Impact, Reason, Stage,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerChildIdentity {
    pub(crate) instance: ComponentInstanceId,
    pub(crate) association: AssociationId,
    pub(crate) task: TaskId,
}
pub(super) struct PeerChild {
    identity: PeerChildIdentity,
    binding: Arc<HostPeerBinding>,
    owner: AsyncMutex<Box<dyn DynResourceLifecycle>>,
    failure: Mutex<Option<SharedError>>,
    receipts: Mutex<CloseReport>,
}
#[derive(Clone)]
pub(crate) struct PeerResourceContext {
    group: Arc<InboundPeerTasks>,
    binding: Arc<HostPeerBinding>,
    ingress: RoleInstanceId,
}
impl std::fmt::Debug for PeerResourceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerResourceContext")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}
struct ColdClaim<'a> {
    group: &'a InboundPeerTasks,
    task: TaskId,
    installed: bool,
}
impl Drop for ColdClaim<'_> {
    fn drop(&mut self) {
        if !self.installed {
            let mut state = self.group.state.lock().expect("peer task state poisoned");
            state.claiming.remove(&self.task);
            state.closing = true;
            state.failure.get_or_insert_with(|| {
                Arc::new(error(
                    Reason::Internal,
                    "cold peer resource factory did not complete",
                ))
            });
            if let Some(binding) = state.active.get(&self.task) {
                binding.request_stop();
            }
            drop(state);
            self.group.notify_changed();
        }
    }
}
impl PeerResourceContext {
    fn cold_failure(&self, failure: ChannelError) -> ChannelError {
        let failure = Arc::new(failure);
        self.group.fail(failure.clone());
        retained(failure)
    }
    pub(crate) fn ingress(&self) -> RoleInstanceId {
        self.ingress
    }
    pub(crate) fn binding(&self) -> Arc<HostPeerBinding> {
        self.binding.clone()
    }
    /// The builder is synchronous and cold: it must neither open I/O nor spawn.
    /// The returned owner is retained before start's first await or native open.
    pub(crate) async fn claim<T>(
        &self,
        deadline: Instant,
        build: impl FnOnce(
            PeerChildIdentity,
        ) -> Result<(Box<dyn DynResourceLifecycle>, T), ChannelError>,
    ) -> Result<T, ChannelError> {
        let task = self.binding.wait_bound().await?;
        {
            let mut state = self.group.state.lock().expect("peer task state poisoned");
            if state.closing || self.binding.cancellation().is_cancelled() {
                return Err(error(Reason::Revoked, "peer resource admission closed"));
            }
            if !state
                .active
                .get(&task)
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.binding))
            {
                return Err(error(
                    Reason::InvalidConfig,
                    "resource belongs to another peer",
                ));
            }
            if state.children.contains_key(&task) || !state.claiming.insert(task) {
                return Err(error(Reason::Capacity, "peer resource budget is one"));
            }
        }
        let mut reservation = ColdClaim {
            group: &self.group,
            task,
            installed: false,
        };
        let instance = self
            .group
            .protocols
            .allocate_resource_identity()
            .map_err(|source| {
                self.cold_failure(
                    error(Reason::Internal, "peer resource identity exhausted").with_source(source),
                )
            })?;
        let identity = PeerChildIdentity {
            instance,
            association: self
                .group
                .protocols
                .allocate_peer_association()
                .map_err(|failure| self.cold_failure(failure))?,
            task,
        };
        let (owner, result) = build(identity).map_err(|failure| self.cold_failure(failure))?;
        let child = Arc::new(PeerChild {
            identity,
            binding: self.binding.clone(),
            owner: AsyncMutex::new(owner),
            failure: Mutex::new(None),
            receipts: Mutex::new(CloseReport::default()),
        });
        let stopped = {
            let mut state = self.group.state.lock().expect("peer task state poisoned");
            state.children.insert(task, child.clone());
            state.claiming.remove(&task);
            reservation.installed = true;
            state.closing || self.binding.cancellation().is_cancelled()
        };
        self.group.notify_changed();
        let mut owner = child.owner.lock().await;
        if stopped {
            owner.revoke(CloseReason::Shutdown);
            return Err(error(
                Reason::Revoked,
                "peer stopped after cold installation",
            ));
        }
        if let Err(failure) = owner.start(deadline).await {
            let failure = child.remember(failure);
            self.group.fail(failure.clone());
            return Err(retained(failure));
        }
        Ok(result)
    }
}
impl PeerChild {
    fn remember(&self, mut error: ChannelError) -> SharedError {
        error
            .component
            .get_or_insert(self.identity.instance.component);
        self.failure
            .lock()
            .expect("peer resource failure poisoned")
            .get_or_insert_with(|| Arc::new(error))
            .clone()
    }
}
impl InboundPeerTasks {
    pub(crate) fn resource_context(
        self: &Arc<Self>,
        binding: Arc<HostPeerBinding>,
        ingress: RoleInstanceId,
    ) -> PeerResourceContext {
        PeerResourceContext {
            group: self.clone(),
            binding,
            ingress,
        }
    }
    fn fail(&self, failure: SharedError) {
        let mut state = self.state.lock().expect("peer task state poisoned");
        state.failure.get_or_insert(failure);
        state.closing = true;
        for binding in state.active.values() {
            binding.request_stop();
        }
        drop(state);
        self.notify_changed();
    }
}

fn error(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
        .with_source(std::io::Error::other(message))
}
