//! Only the real registered scope can publish a peer's join receipt.
use super::*;
use crate::component::instance::{retained, SharedError};
use tokio::sync::Notify;
mod observer;
pub(crate) use observer::HostPeerJoinObserver;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{scope::TaskClass, ChannelError, Impact, Reason, Stage};

#[derive(Debug)]
pub(crate) struct JoinedPeer {
    task: TaskId,
    class: TaskClass,
    failure: Option<SharedError>,
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
}
#[derive(Default)]
struct JoinCell {
    state: Mutex<State>,
    changed: Notify,
}
pub(crate) struct HostPeerBinding {
    parent: ComponentInstanceId,
    join: Arc<JoinCell>,
    cancel: CancellationToken,
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
    pub(super) fn new(parent: ComponentInstanceId, cancel: CancellationToken) -> Self {
        Self {
            parent,
            join: Arc::default(),
            cancel: cancel.child_token(),
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
