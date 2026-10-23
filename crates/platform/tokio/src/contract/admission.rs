//! Admission ownership for roles whose I/O is transferred to each session.
//! Shared pools, listeners, devices and drivers need their own resource owner.
use super::{
    context::OpenContext,
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::Instant;
use zero_core::component::{ComponentId, SessionId};

pub struct SessionAdmission {
    component: ComponentId,
    ready: AtomicBool,
    cancel: CancellationToken,
}
pub struct SessionOwner(Arc<SessionAdmission>);
impl SessionOwner {
    pub fn new(
        component: ComponentId,
        parent: &CancellationToken,
    ) -> (Self, Arc<SessionAdmission>) {
        let state = Arc::new(SessionAdmission {
            component,
            ready: AtomicBool::new(false),
            cancel: parent.child_token(),
        });
        (Self(state.clone()), state)
    }
}
impl SessionAdmission {
    pub fn check(&self, context: &OpenContext) -> Result<Option<SessionId>, ChannelError> {
        let session = context.session_identity()?;
        let reason = if !self.ready.load(Ordering::Acquire) || self.cancel.is_cancelled() {
            Some(Reason::Revoked)
        } else if context.cancel.is_cancelled() {
            Some(Reason::Cancelled)
        } else if context.deadline_elapsed() {
            Some(Reason::Timeout)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(self.error(session, Stage::Dial, reason, DeliveryState::NotSubmitted));
        }
        Ok(session)
    }
    pub fn attach(&self, mut error: ChannelError, session: Option<SessionId>) -> ChannelError {
        error.component.get_or_insert(self.component);
        if error.session.is_none() {
            error.session = session;
        }
        error
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
    pub fn error(
        &self,
        session: Option<SessionId>,
        stage: Stage,
        reason: Reason,
        delivery: DeliveryState,
    ) -> ChannelError {
        let mut error = ChannelError::new(stage, reason, Impact::Session);
        error.component = Some(self.component);
        error.session = session;
        error.delivery = delivery;
        error
    }
}
impl Drop for SessionOwner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for SessionOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let reason = if self.0.cancel.is_cancelled() {
            Some(Reason::Revoked)
        } else if Instant::now() >= deadline {
            Some(Reason::Timeout)
        } else {
            None
        };
        if let Some(reason) = reason {
            let mut error = self
                .0
                .error(None, Stage::Prepare, reason, DeliveryState::NotSubmitted);
            error.impact = Impact::Component;
            return Err(error);
        }
        self.0.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.ready.store(false, Ordering::Release);
        self.0.cancel.cancel();
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        // No shared I/O/tasks exist here; transferred streams remain with sessions.
        Ok(CloseReport::default())
    }
}
