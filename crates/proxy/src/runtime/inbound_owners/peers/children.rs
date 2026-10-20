//! One cold resource per actual peer; the existing peer permit includes retirement.
use super::*;
use crate::component::instance::{retained, SharedError};
use tokio::sync::Mutex as AsyncMutex;
use zero_core::component::{AssociationId, RoleInstanceId};
use zero_platform_tokio::contract::{
    lifecycle::DynResourceLifecycle, ChannelError, CloseReason, Impact, Reason, Stage,
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
                error(Reason::Internal, "peer resource identity exhausted").with_source(source)
            })?;
        let identity = PeerChildIdentity {
            instance,
            association: self.group.protocols.allocate_peer_association()?,
            task,
        };
        let (owner, result) = build(identity).map_err(|failure| {
            let failure = Arc::new(failure);
            self.group.fail(failure.clone());
            retained(failure)
        })?;
        let child = Arc::new(PeerChild {
            identity,
            binding: self.binding.clone(),
            owner: AsyncMutex::new(owner),
            failure: Mutex::new(None),
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
    /// Only the original scope's dispatched TaskExit makes a child eligible.
    /// This method may be cancelled and retried: actual owners stay in the map.
    pub(crate) async fn retire_joined(&self, deadline: Instant) -> Result<(), ChannelError> {
        let children = {
            let state = self.state.lock().expect("peer task state poisoned");
            state
                .children
                .iter()
                .filter(|(_, child)| child.binding.joined_receipt().is_some())
                .map(|(task, child)| (*task, child.clone()))
                .collect::<Vec<_>>()
        };
        for (task, child) in children {
            let mut owner = tokio::time::timeout_at(deadline, child.owner.lock())
                .await
                .map_err(|source| {
                    error(Reason::Timeout, "peer resource cleanup is busy").with_source(source)
                })?;
            // A concurrent cleanup may have consumed this same confirmed owner.
            if !self
                .state
                .lock()
                .expect("peer task state poisoned")
                .children
                .get(&task)
                .is_some_and(|actual| Arc::ptr_eq(actual, &child))
            {
                continue;
            }
            owner.revoke(CloseReason::Shutdown);
            let mut report = match owner.shutdown(deadline).await {
                Ok(report) => report,
                Err(failure) => {
                    let failure = child.remember(failure);
                    self.fail(failure.clone());
                    return Err(retained(failure));
                }
            };
            if let Some(failed) = report.failed_tasks.pop() {
                let failure = child.remember(failed.error);
                self.fail(failure.clone());
                return Err(retained(failure));
            }
            if !report.is_complete() {
                return Err(error(
                    Reason::Timeout,
                    "peer native resource remains unconfirmed",
                ));
            }
            let mut state = self.state.lock().expect("peer task state poisoned");
            state.confirmed_children = state.confirmed_children.saturating_add(1);
            state.closed_io = state.closed_io.saturating_add(report.closed_io.len());
            // Successful evidence has been consumed here. Retain bounded
            // totals, not one historical string/owner per completed peer.
            state.children.remove(&task);
            state.active.remove(&task);
            state.reclaimed = state.reclaimed.saturating_add(1);
            drop(state);
            self.notify_changed();
        }
        Ok(())
    }
}
fn error(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
        .with_source(std::io::Error::other(message))
}
