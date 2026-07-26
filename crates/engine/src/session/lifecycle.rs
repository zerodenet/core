//! RAII session lifecycle handle.

use crate::observability::SessionOutcome;
use crate::runtime::Engine;

use super::{CompletedSessionRecord, FlowFailureObservation};

#[derive(Debug)]
pub struct SessionHandle {
    engine: Engine,
    session_id: u64,
    finished: bool,
}

impl SessionHandle {
    pub fn new(engine: Engine, session_id: u64) -> Self {
        Self {
            engine,
            session_id,
            finished: false,
        }
    }

    pub fn finish(&mut self, outcome: SessionOutcome) -> Option<CompletedSessionRecord> {
        self.finish_with_reason(outcome, None)
    }

    pub fn register_cancellation<F>(&self, cancel: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        self.engine
            .register_session_cancellation(self.session_id, cancel)
    }

    pub fn is_cancelled(&self) -> bool {
        self.engine.session_is_cancelled(self.session_id)
    }

    pub fn cancellation_reason(&self) -> Option<String> {
        self.engine.session_cancellation_reason(self.session_id)
    }

    /// Finish the session with an explicit close reason.
    pub fn finish_with_reason(
        &mut self,
        outcome: SessionOutcome,
        close_reason: Option<String>,
    ) -> Option<CompletedSessionRecord> {
        if self.finished {
            return None;
        }

        let record = self
            .engine
            .finish_session_with_reason(self.session_id, outcome, close_reason);
        self.finished = true;
        record
    }

    pub fn finish_with_failure(
        &mut self,
        close_reason: impl Into<String>,
        failure: FlowFailureObservation,
    ) -> Option<CompletedSessionRecord> {
        if self.finished {
            return None;
        }

        let record = self.engine.finish_session_with_observation(
            self.session_id,
            SessionOutcome::Failed,
            Some(close_reason.into()),
            Some(failure),
        );
        self.finished = true;
        record
    }
}

impl Drop for SessionHandle {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.engine.finish_session_with_reason(
                self.session_id,
                SessionOutcome::Failed,
                None,
            );
        }
    }
}
