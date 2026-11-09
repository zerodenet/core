//! Bounded peer registrations belong to the actual persistent listener scope.
use super::*;
use crate::component::instance::{retained, SharedError};
use std::{collections::BTreeMap, future::Future};
use tokio::sync::Notify;
use zero_core::component::TaskId;
pub(super) mod children;
mod failures;

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
    closed_io_receipts: usize,
}
pub(crate) struct InboundPeerTasks {
    identity: ComponentInstanceId,
    protocols: ProtocolInventory,
    limit: NonZeroUsize,
    cancel: CancellationToken,
    state: Mutex<State>,
    changed: Notify,
    client_task_failures: Option<zero_platform_tokio::AcceptedTaskFailures>,
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
        drop(state);
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
            .field("closed_io_receipts", &state.closed_io_receipts)
            .field("closing", &state.closing)
            .finish_non_exhaustive()
    }
}
impl InboundPeerTasks {
    #[cfg(test)]
    pub(crate) fn diagnose_actual_retirement(&self) {
        let (preparing, children, claiming, fault, bindings) = {
            let state = self.state.lock().unwrap();
            (
                state.preparing,
                state.children.len(),
                state.claiming.len(),
                state.failure.clone(),
                state
                    .active
                    .iter()
                    .map(|(id, binding)| (*id, binding.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        eprintln!("actual H3 source peer census: preparing={preparing} children={children} claiming={claiming} fault={fault:?}");
        for (id, binding) in bindings {
            eprintln!("actual H3 peer {id:?}: primaryJoined={} dispatched={} responsesPending={} processRetired={} stop={}",
                binding.join_witness().is_some_and(|witness| witness.is_joined()),
                binding.joined_receipt().is_some(), binding.responses_pending(),
                binding.process_retired(), binding.cancellation().is_cancelled());
        }
    }
    /// Actual per-source association custody only; no task/native-close grant.
    pub(crate) fn preflight_quiescent(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.closing
            && state.failure.is_none()
            && state.active.is_empty()
            && state.preparing == 0
            && state.children.is_empty()
            && state.claiming.is_empty()
    }

    /// Actual original peer/child tables only. Not native or Scope closure.
    pub(super) fn acquisition_quiescent(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active.is_empty()
            && state.preparing == 0
            && state.children.is_empty()
            && state.claiming.is_empty()
    }

    #[cfg(test)]
    pub(super) fn observe_primary_joined_for_test(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        self.state
            .lock()
            .unwrap()
            .active
            .get(&task)?
            .join_witness()?
            .try_joined()
    }
    pub(super) fn observe_peer_joined(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        let bindings = self
            .state
            .lock()
            .unwrap()
            .active
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let joined = bindings.into_iter().find_map(|binding| {
            let primary = binding.receive_primary_joined(task);
            let process = binding.observe_process_joined(task);
            // Both owning receivers may hold this same helper. Never skip the
            // Users registration/book because the binding received it first.
            let peer = binding
                .peer_scope()
                .and_then(|scope| scope.observe_joined(task));
            process.or(peer).or(primary)
        });
        if joined.is_some() {
            self.notify_changed();
        }
        joined
    }
    pub(super) async fn retire_logical_resources(
        &self,
        deadline: Instant,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let scopes: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .active
            .values()
            .filter_map(|b| b.peer_scope())
            .collect();
        for scope in scopes {
            // Association dispatcher state lives in its retained child owner.
            // That owner releases actual flow views before retiring this same
            // graph. Closing the graph first would wait on its own held views.
            if scope.origin() == zero_platform_tokio::AcceptedOrigin::Association {
                continue;
            }
            scope.retire_logical_resources(deadline).await?;
        }
        Ok(())
    }
    pub(super) fn new(
        identity: ComponentInstanceId,
        cancel: CancellationToken,
        protocols: ProtocolInventory,
    ) -> Self {
        Self {
            identity,
            protocols,
            limit: NonZeroUsize::new(MAX_DATAGRAM_PEERS).unwrap(),
            cancel,
            state: Mutex::default(),
            changed: Notify::new(),
            client_task_failures: None,
        }
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) fn with_limit(
        identity: ComponentInstanceId,
        cancel: CancellationToken,
        protocols: ProtocolInventory,
        limit: NonZeroUsize,
    ) -> Self {
        let mut group = Self::new(identity, cancel, protocols);
        group.limit = limit;
        group
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) fn declared_limit(&self) -> usize {
        self.limit.get()
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn occupied(&self) -> usize {
        let state = self.state.lock().expect("peer task state poisoned");
        state.active.len() + state.preparing
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
        let Some(mut task_failure) = self.reserve_task_failure(host)? else {
            return Ok(false);
        };
        let binding = {
            let mut state = self.state.lock().expect("peer task state poisoned");
            if state.closing || state.active.len() + state.preparing >= self.limit.get() {
                return Ok(false);
            }
            state.preparing += 1;
            Arc::new(HostPeerBinding::new(
                self.identity,
                self.cancel.clone(),
                host.peer_registrar(),
                Arc::downgrade(&self.protocols.runtime_host().original_budgets().carrier),
            ))
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
        task_failure.begin_registration();
        let id = host.spawn_critical_tracked(task);
        let (id, witness) = match id {
            Ok(registered) => registered,
            Err(source) => {
                task_failure.known_not_registered();
                use zero_platform_tokio::contract::{scope::SpawnError, Reason};
                let reason = match source {
                    SpawnError::Capacity => Reason::Capacity,
                    SpawnError::Closing => Reason::Revoked,
                    SpawnError::NotStarted => Reason::InvalidConfig,
                    SpawnError::IdentityExhausted => Reason::Internal,
                };
                let mut failure = zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    reason,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source);
                if reason == Reason::Capacity {
                    failure.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
                }
                let source = Arc::new(failure);
                if reason == Reason::Capacity {
                    // This actual registrar rejected the task before polling
                    // any native work. Release only its cold reservation;
                    // later real joins may make the same live group usable.
                    state.preparing -= 1;
                    registration.accepted = true;
                }
                drop(state);
                binding.reject(source.clone());
                return Err(std::io::Error::other(retained(source)).into());
            }
        };
        // Receipt dispatch takes this same lock, including exit-before-insert.
        let task_failure = task_failure
            .registered(&witness)
            .unwrap_or_else(|_| panic!("same original peer registration prepaid before spawn"));
        binding.install_task_failure(task_failure);
        binding.bind(id, witness);
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
        let binding = {
            let mut state = self.state.lock().expect("peer task state poisoned");
            let Some(binding) = state.active.get(&id).cloned() else {
                return false;
            };
            // Persist the actual source before releasing locks/callbacks. A
            // real joined failure cannot vanish if a woken cleanup progresses.
            if let Some(source) = &failure {
                state.failure.get_or_insert_with(|| source.clone());
                state.closing = true;
            }
            binding
        };
        binding.joined(id, class, failure);
        let removed = {
            let mut state = self.state.lock().expect("peer task state poisoned");
            if !state.children.contains_key(&id)
                && !state.claiming.contains(&id)
                && !binding.responses_pending()
                && binding.process_retired()
                && state
                    .active
                    .get(&id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, &binding))
            {
                state.reclaimed = state.reclaimed.saturating_add(1);
                state.active.remove(&id)
            } else {
                None
            }
        };
        // Last original/view/waker drops and all stop notifications are
        // outside the same existing group's mutex.
        drop(removed);
        self.notify_changed();
        true
    }
    pub(super) fn first_fault(&self) -> Option<SharedError> {
        self.state
            .lock()
            .expect("peer task state poisoned")
            .failure
            .clone()
    }
    pub(super) fn physically_closed(&self) -> bool {
        let state = self.state.lock().expect("peer task state poisoned");
        state.closing
            && state.active.is_empty()
            && state.preparing == 0
            && state.children.is_empty()
            && state.claiming.is_empty()
    }
    pub(super) fn closed_io_receipts_count(&self) -> usize {
        self.state
            .lock()
            .expect("peer task state poisoned")
            .closed_io_receipts
    }
    pub(crate) fn begin_shutdown(&self) {
        let bindings = {
            let mut state = self.state.lock().expect("peer task state poisoned");
            state.closing = true;
            state.active.values().cloned().collect::<Vec<_>>()
        };
        for binding in bindings {
            binding.request_stop();
        }
        self.notify_changed();
    }
    fn notify_changed(&self) {
        self.changed.notify_waiters();
        self.changed.notify_one();
    }
    #[cfg(test)]
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
