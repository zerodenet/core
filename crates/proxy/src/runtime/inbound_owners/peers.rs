//! Bounded peer registrations belong to the actual persistent listener scope.
use super::*;
use crate::component::instance::{retained, SharedError};
use std::{collections::BTreeMap, future::Future};
use tokio::sync::Notify;
use zero_core::component::TaskId;
pub(super) mod children;

// Preserve the actual Direct peer listener's existing admission bound.
pub(crate) const MAX_DATAGRAM_PEERS: usize = 4096;
#[derive(Default)]
struct State {
    active: BTreeMap<TaskId, Arc<HostPeerBinding>>,
    preparing: usize,
    children: BTreeMap<TaskId, Arc<children::PeerChild>>,
    claiming: std::collections::BTreeSet<TaskId>,
    closing: bool,
    failure: Option<SharedError>,
    reclaimed: usize,
    confirmed_children: usize,
    closed_io: usize,
}
pub(crate) struct InboundPeerTasks {
    identity: ComponentInstanceId,
    protocols: ProtocolInventory,
    cancel: CancellationToken,
    state: Mutex<State>,
    changed: Notify,
}
struct Registration<'a> {
    group: &'a InboundPeerTasks,
    binding: Arc<HostPeerBinding>,
    accepted: bool,
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        if self.accepted {
            return;
        }
        let mut state = self
            .group
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.preparing -= 1;
        // A cold factory may panic before an actual task is accepted. Reject
        // its admission, never invent a joined TaskId or an I/O close report.
        state.closing = true;
        self.binding.reject(Arc::new(
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(std::io::Error::other(
                "peer registration ended before scope acceptance",
            )),
        ));
        drop(state);
        self.group.notify_changed();
    }
}
impl std::fmt::Debug for InboundPeerTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("peer task state poisoned");
        f.debug_struct("InboundPeerTasks")
            .field("active", &state.active.len())
            .field("reclaimed", &state.reclaimed)
            .field("confirmed_children", &state.confirmed_children)
            .field("closed_io", &state.closed_io)
            .field("closing", &state.closing)
            .finish_non_exhaustive()
    }
}
impl InboundPeerTasks {
    pub(super) fn new(
        identity: ComponentInstanceId,
        cancel: CancellationToken,
        protocols: ProtocolInventory,
    ) -> Self {
        Self {
            identity,
            protocols,
            cancel,
            state: Mutex::default(),
            changed: Notify::new(),
        }
    }
    #[cfg(test)]
    pub(crate) fn spawn<F, Fut>(
        &self,
        host: &InboundTaskAccess,
        make: F,
    ) -> Result<bool, EngineError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        self.spawn_bound(host, |_| {
            let task = make();
            async move {
                if let Err(error) = task.await {
                    tracing::debug!(%error, "UDP peer association ended");
                }
                Ok(())
            }
        })
    }
    pub(crate) fn spawn_bound<F, Fut>(
        &self,
        host: &InboundTaskAccess,
        make: F,
    ) -> Result<bool, EngineError>
    where
        F: FnOnce(Arc<HostPeerBinding>) -> Fut,
        Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        if host.identity != self.identity {
            return Err(std::io::Error::other(
                "peer group belongs to another actual listener scope",
            )
            .into());
        }
        let binding = {
            let mut state = self.state.lock().expect("peer task state poisoned");
            if state.closing || state.active.len() + state.preparing >= MAX_DATAGRAM_PEERS {
                return Ok(false);
            }
            state.preparing += 1;
            Arc::new(HostPeerBinding::new(self.identity, self.cancel.clone()))
        };
        let mut registration = Registration {
            group: self,
            binding: binding.clone(),
            accepted: false,
        };
        // Never run a protocol/cold-owner factory under the registration lock.
        // Its bounded reservation persists if it panics or stop races it.
        let task = make(binding.clone());
        let mut state = self.state.lock().expect("peer task state poisoned");
        if state.closing || self.cancel.is_cancelled() {
            drop(state);
            return Ok(false);
        }
        let id = host.spawn_critical(task);
        let id = match id {
            Ok(id) => id,
            Err(source) => {
                use zero_platform_tokio::contract::{scope::SpawnError, Reason};
                let reason = match source {
                    SpawnError::Capacity => Reason::Capacity,
                    SpawnError::Closing => Reason::Revoked,
                    SpawnError::NotStarted => Reason::InvalidConfig,
                    SpawnError::IdentityExhausted => Reason::Internal,
                };
                let source = Arc::new(
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        reason,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source),
                );
                binding.reject(source.clone());
                drop(state);
                return Err(std::io::Error::other(retained(source)).into());
            }
        };
        // Receipt dispatch takes this same lock, including exit-before-insert.
        binding.bind(id);
        state.active.insert(id, binding);
        state.preparing -= 1;
        registration.accepted = true;
        Ok(true)
    }
    pub(super) fn joined(
        &self,
        id: TaskId,
        class: zero_platform_tokio::contract::scope::TaskClass,
        failure: Option<SharedError>,
    ) -> bool {
        let mut state = self.state.lock().expect("peer task state poisoned");
        let Some(binding) = state.active.get(&id).cloned() else {
            return false;
        };
        binding.joined(id, class, failure.clone());
        if let Some(failure) = failure {
            state.failure.get_or_insert(failure);
            state.closing = true;
        }
        if !state.children.contains_key(&id) && !state.claiming.contains(&id) {
            state.active.remove(&id);
            state.reclaimed = state.reclaimed.saturating_add(1);
        }
        drop(state);
        self.notify_changed();
        true
    }
    pub(crate) fn begin_shutdown(&self) {
        let mut state = self.state.lock().expect("peer task state poisoned");
        state.closing = true;
        for binding in state.active.values() {
            binding.request_stop();
        }
        self.notify_changed();
    }
    fn notify_changed(&self) {
        self.changed.notify_waiters();
        self.changed.notify_one();
    }
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }
    pub(crate) async fn wait_closed(&self) -> Result<(), EngineError> {
        self.begin_shutdown();
        loop {
            let notified = self.changed.notified();
            let cleanup = self
                .retire_joined(Instant::now() + std::time::Duration::from_secs(5))
                .await;
            {
                let state = self.state.lock().expect("peer task state poisoned");
                if state.preparing == 0
                    && state
                        .active
                        .values()
                        .all(|binding| binding.joined_receipt().is_some())
                {
                    // A joined Unknown resource remains owned, but must not
                    // prevent dispatch of other peers' actual pending exits.
                    cleanup.map_err(|error| std::io::Error::other(retained(Arc::new(error))))?;
                    if state.active.is_empty() {
                        return state.failure.as_ref().map_or(Ok(()), |error| {
                            Err(std::io::Error::other(retained(error.clone())).into())
                        });
                    }
                }
            }
            notified.await;
        }
    }
}
