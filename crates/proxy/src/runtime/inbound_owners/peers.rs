//! Bounded peer registrations belong to the actual persistent listener scope.
use super::*;
use crate::component::instance::{retained, SharedError};
use std::{collections::BTreeSet, future::Future};
use tokio::sync::Notify;
use zero_core::component::TaskId;

// Preserve the actual Direct peer listener's existing admission bound.
pub(crate) const MAX_DATAGRAM_PEERS: usize = 4096;
#[derive(Default)]
struct State {
    active: BTreeSet<TaskId>,
    closing: bool,
    failure: Option<SharedError>,
    reclaimed: usize,
}
pub(crate) struct InboundPeerTasks {
    state: Mutex<State>,
    changed: Notify,
}
impl std::fmt::Debug for InboundPeerTasks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("peer task state poisoned");
        f.debug_struct("InboundPeerTasks")
            .field("active", &state.active.len())
            .field("reclaimed", &state.reclaimed)
            .field("closing", &state.closing)
            .finish_non_exhaustive()
    }
}
impl InboundPeerTasks {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::default(),
            changed: Notify::new(),
        }
    }
    pub(crate) fn spawn<F, Fut>(
        &self,
        host: &InboundTaskAccess,
        make: F,
    ) -> Result<bool, EngineError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        let mut state = self.state.lock().expect("peer task state poisoned");
        if state.closing || state.active.len() >= MAX_DATAGRAM_PEERS {
            return Ok(false);
        }
        let task = make();
        let id = host.spawn(async move {
            if let Err(error) = task.await {
                tracing::debug!(%error, "UDP peer association ended");
            }
            Ok(())
        })?;
        // Receipt dispatch takes this same lock, including exit-before-insert.
        state.active.insert(id);
        Ok(true)
    }
    pub(super) fn joined(&self, id: TaskId, failure: Option<SharedError>) -> bool {
        let mut state = self.state.lock().expect("peer task state poisoned");
        if !state.active.remove(&id) {
            return false;
        }
        if let Some(failure) = failure {
            state.failure.get_or_insert(failure);
            state.closing = true;
        } else {
            state.reclaimed = state.reclaimed.saturating_add(1);
        }
        drop(state);
        self.changed.notify_one();
        true
    }
    pub(crate) fn begin_shutdown(&self) {
        self.state.lock().expect("peer task state poisoned").closing = true;
        self.changed.notify_one();
    }
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }
    pub(crate) async fn wait_closed(&self) -> Result<(), EngineError> {
        self.begin_shutdown();
        loop {
            let notified = self.changed.notified();
            {
                let state = self.state.lock().expect("peer task state poisoned");
                if state.active.is_empty() {
                    return state.failure.as_ref().map_or(Ok(()), |error| {
                        Err(std::io::Error::other(retained(error.clone())).into())
                    });
                }
            }
            notified.await;
        }
    }
}
