//! The existing accepted task or JoinSet retains the original registration.
use super::*;
impl FrontendCommand {
    pub(crate) fn from_original(original: Arc<Original>) -> Self {
        Self(original)
    }
    pub fn register_original_connection(&self) -> bool {
        let task = match lock(&self.0.credit).as_ref() {
            Some(Credit::Connection(credit)) => credit.original_command_task(),
            _ => None,
        };
        task.is_some_and(|task| self.register_inline(task))
    }
    pub fn observation(&self) -> ApplyObservation {
        self.0.observation.clone()
    }
    /// Bind the exact opaque registration published by the already-installed
    /// accepted handler or RPC stream. Never constructs a TaskJoined.
    pub fn register_scoped(&self, actual: TaskJoinWitness) -> bool {
        if actual.task_id().is_none()
            || self
                .0
                .registration
                .set(Registration::Scoped(actual.clone()))
                .is_err()
        {
            return false;
        }
        *lock(&self.0.caller_ack) = Some(Box::pin(async move {
            actual.wait_registration_released().await;
        }));
        true
    }
    /// Called by the original JoinSet owner before opening its existing gate.
    pub fn register_joinset(&self, actual: tokio::task::AbortHandle) -> bool {
        self.0
            .registration
            .set(Registration::JoinSet(actual))
            .is_ok()
    }
    /// This command has no child task: its exact inline continuation runs in
    /// this actual connection registration, whose IO/Scope remain independent.
    pub fn register_inline(&self, actual: tokio::task::AbortHandle) -> bool {
        self.0
            .registration
            .set(Registration::Inline(actual))
            .is_ok()
    }
    /// The original JoinSet consumer calls this only after consuming this same
    /// task's real result. The ID is matched to its original opaque handle.
    pub fn joined_by_original_receiver(&self, id: tokio::task::Id) -> bool {
        if !matches!(self.0.registration.get(),Some(Registration::JoinSet(actual)) if actual.id()==id)
        {
            return false;
        }
        self.0.caller_returned.store(true, Ordering::Release);
        if let Some(owner) = self.0.owner.upgrade() {
            owner.release_returned_frontends();
        }
        self.0.notify();
        true
    }
    /// Successful/failed return of the same inline continuation, after actual
    /// command cleanup. This is logical return, never the connection's join.
    pub async fn finish_inline(&self) -> Result<(), ChannelError> {
        match self.0.registration.get() {
            Some(Registration::Inline(actual)) if Some(actual.id()) == tokio::task::try_id() => {}
            Some(Registration::Scoped(_)) => {
                return self.0.finish_unstarted().await;
            }
            Some(Registration::JoinSet(_)) => return self.0.finish_unstarted().await,
            None if self.0.raw.get().is_none() && !self.0.creating.load(Ordering::Acquire) => {}
            _ => return Err(fault(Reason::StalePlan)),
        }
        self.0.finish_unstarted().await?;
        self.0.caller_returned.store(true, Ordering::Release);
        self.0.notify();
        Ok(())
    }
}
