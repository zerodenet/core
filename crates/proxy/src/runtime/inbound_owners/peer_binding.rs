//! Only the real registered scope can publish a peer's join receipt.
use super::*;
use crate::component::instance::{retained, SharedError};
use tokio::sync::Notify;
mod failures;
mod observer;
mod packet_receiver;
mod process;
mod responses;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{scope::TaskClass, ChannelError, Impact, Reason, Stage};

pub(crate) struct JoinedPeer {
    task: TaskId,
    class: TaskClass,
    failure: Option<SharedError>,
    // The original observer/result may outlive Binding. Keep the same paid
    // task-header responsibility after its actual registration has ended.
    _task_failure: Option<zero_platform_tokio::AcceptedTaskFailureRegistration>,
}
impl std::fmt::Debug for JoinedPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedPeer")
            .field("task", &self.task)
            .field("class", &self.class)
            .field("failure", &self.failure)
            .finish()
    }
}
impl JoinedPeer {
    pub(crate) fn task(&self) -> TaskId {
        self.task
    }
    pub(crate) fn class(&self) -> TaskClass {
        self.class
    }
    pub(crate) fn failure(&self) -> Option<SharedError> {
        self.failure.clone()
    }
}
#[derive(Default)]
struct State {
    task: Option<TaskId>,
    witness: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    joined: Option<Arc<JoinedPeer>>,
    rejected: Option<SharedError>,
    // JoinObserver shares this exact cell, without holding Binding or Scope.
    task_failure: Option<zero_platform_tokio::AcceptedTaskFailureRegistration>,
}
#[derive(Default)]
struct JoinCell {
    state: Mutex<State>,
    changed: Notify,
}
pub(crate) struct HostPeerBinding {
    parent: ComponentInstanceId,
    registrar: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    lookup_root: std::sync::Weak<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    process: Mutex<Option<Arc<zero_platform_tokio::LocalProcessLookup>>>,
    process_failure: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoined>>,
    process_cleanup_failure: Mutex<Option<Arc<ChannelError>>>,
    join: Arc<JoinCell>,
    cancel: CancellationToken,
    responses: Mutex<Option<responses::ResponseOwner>>,
    peer_scope: Mutex<Option<zero_platform_tokio::AcceptedPeerScope>>,
    task_failure: std::sync::OnceLock<zero_platform_tokio::AcceptedTaskFailureRegistration>,
    // Last: the original lookup/error witnesses above drop before their paid
    // Scope-header responsibility. This is the same Source book, not a pool.
    process_task_failure: Mutex<Option<zero_platform_tokio::AcceptedTaskFailureRegistration>>,
}
impl HostPeerBinding {
    pub(crate) fn protocol_metadata(
        &self,
    ) -> Result<
        (
            zero_platform_tokio::contract::component::PacketDriverMetadataView,
            usize,
        ),
        ChannelError,
    > {
        if self.cancel.is_cancelled()
            || self
                .join_witness()
                .is_none_or(|witness| witness.is_joined())
        {
            return Err(self.error(Reason::Revoked, "actual protocol caller stopped"));
        }
        let receiver = self
            .task_failure
            .get()
            .ok_or_else(|| self.error(Reason::Revoked, "actual Source book is absent"))?
            .receiver();
        if receiver.instance() != self.parent
            || !receiver
                .scope_identity()
                .same_scope(&self.registrar.scope_identity())
        {
            return Err(self.error(
                Reason::StalePlan,
                "protocol metadata belongs to another actual Source",
            ));
        }
        let root = self
            .lookup_root
            .upgrade()
            .ok_or_else(|| self.error(Reason::Revoked, "actual Host root retired"))?;
        Ok((
            receiver.metadata_view().clone(),
            root.snapshot().limit.get(),
        ))
    }
}
impl std::fmt::Debug for HostPeerBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.join.state.lock().expect("peer binding poisoned");
        f.debug_struct("HostPeerBinding")
            .field("parent", &self.parent)
            .field("task", &state.task)
            .field("joined", &state.joined.is_some())
            .finish_non_exhaustive()
    }
}
impl HostPeerBinding {
    pub(crate) fn install_scope(
        &self,
        scope: zero_platform_tokio::AcceptedPeerScope,
    ) -> Result<(), ChannelError> {
        let original = self.task_failure.get().ok_or_else(|| {
            self.error(
                Reason::InvalidConfig,
                "peer has no original paid task-fault registration",
            )
        })?;
        scope.bind_task_failure_registration(original)?;
        let root = self
            .lookup_root
            .upgrade()
            .ok_or_else(|| self.error(Reason::Revoked, "actual lookup root retired"))?;
        scope.bind_process_lookup_totals(self.parent, &self.registrar, &root)?;
        let mut slot = self.peer_scope.lock().unwrap();
        assert!(slot.is_none(), "one real peer scope");
        *slot = Some(scope);
        Ok(())
    }
    pub(super) fn peer_scope(&self) -> Option<zero_platform_tokio::AcceptedPeerScope> {
        self.peer_scope.lock().unwrap().clone()
    }
    pub(super) fn new(
        parent: ComponentInstanceId,
        cancel: CancellationToken,
        registrar: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        lookup_root: std::sync::Weak<
            zero_platform_tokio::controlled_carrier::CarrierOriginalTotals,
        >,
    ) -> Self {
        Self {
            parent,
            registrar,
            lookup_root,
            process: Mutex::new(None),
            process_failure: Mutex::new(None),
            process_cleanup_failure: Mutex::new(None),
            join: Arc::default(),
            cancel: cancel.child_token(),
            responses: Mutex::new(None),
            peer_scope: Mutex::new(None),
            task_failure: std::sync::OnceLock::new(),
            process_task_failure: Mutex::new(None),
        }
    }
    pub(super) fn bind(
        &self,
        task: TaskId,
        witness: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) {
        let mut state = self.join.state.lock().expect("peer binding poisoned");
        assert!(
            state.task.is_none() && state.rejected.is_none(),
            "one actual scope registration"
        );
        state.task = Some(task);
        state.witness = Some(witness);
        drop(state);
        self.join.changed.notify_waiters();
    }
    pub(super) fn reject(&self, error: SharedError) {
        let mut state = self.join.state.lock().expect("peer binding poisoned");
        assert!(state.task.is_none(), "accepted task cannot be rejected");
        state.rejected.get_or_insert(error);
        drop(state);
        self.request_stop();
        self.join.changed.notify_waiters();
    }
    pub(super) fn joined(&self, task: TaskId, class: TaskClass, failure: Option<SharedError>) {
        let mut state = self.join.state.lock().expect("peer binding poisoned");
        assert_eq!(
            state.task,
            Some(task),
            "receipt must match the actual registered task"
        );
        assert!(state.joined.is_none(), "one actual join receipt");
        state.joined = Some(Arc::new(JoinedPeer {
            task,
            class,
            failure,
            _task_failure: state.task_failure.clone(),
        }));
        drop(state);
        self.request_stop();
        self.join.changed.notify_waiters();
    }
    pub(super) fn joined_receipt(&self) -> Option<Arc<JoinedPeer>> {
        self.join
            .state
            .lock()
            .expect("peer binding poisoned")
            .joined
            .clone()
    }
    /// The exact witness returned by the same actual spawn_tracked call.
    /// Observation neither consumes TaskExit nor confirms any native I/O.
    pub(crate) fn join_witness(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        self.join
            .state
            .lock()
            .expect("peer binding poisoned")
            .witness
            .clone()
    }
    /// Observation only: only the original containing Scope constructs this.
    /// It proves no native descriptor or transferred-user closure.
    pub(crate) fn observed_join(&self) -> Option<Arc<JoinedPeer>> {
        self.joined_receipt()
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub(crate) fn request_stop(&self) {
        self.cancel.cancel();
        let original = self.process.lock().unwrap().clone();
        if let Some(original) = original {
            original.cancel();
        }
    }
    pub(crate) fn registration_rejection(&self) -> Option<SharedError> {
        self.join
            .state
            .lock()
            .expect("peer binding poisoned")
            .rejected
            .clone()
    }
    pub(crate) async fn wait_bound(&self) -> Result<TaskId, ChannelError> {
        loop {
            let changed = self.join.changed.notified();
            {
                let state = self.join.state.lock().expect("peer binding poisoned");
                if let Some(error) = &state.rejected {
                    return Err(retained(error.clone()));
                }
                if self.cancel.is_cancelled() {
                    return Err(
                        self.error(Reason::Revoked, "peer stopped before native activation")
                    );
                }
                if let Some(task) = state.task {
                    return Ok(task);
                }
            }
            tokio::select! { _=changed=>{}, _=self.cancel.cancelled()=>{} }
        }
    }
    pub(crate) async fn wait_joined(
        &self,
        deadline: Instant,
    ) -> Result<Arc<JoinedPeer>, ChannelError> {
        self.join_observer().wait_joined(deadline).await
    }
    fn error(&self, reason: Reason, message: &'static str) -> ChannelError {
        let mut error = ChannelError::new(Stage::Close, reason, Impact::Component)
            .with_source(std::io::Error::other(message));
        error.component = Some(self.parent.component);
        error
    }
}
