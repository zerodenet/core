//! Successful Engine admission carried by one real ingress invocation.
//! This is not native resource, component, prefix or socket creation authority.
use zero_core::component::SessionId;
use zero_engine::{AdmittedSession, Engine, EngineError};
use zero_platform_tokio::contract::CancellationToken;

#[derive(Clone)]
pub(crate) struct AdmittedTcpDemand {
    admission: AdmittedSession,
    cancel: CancellationToken,
}
impl AdmittedTcpDemand {
    pub(crate) fn new(
        engine: &Engine,
        session: SessionId,
        admission: AdmittedSession,
    ) -> Result<Self, EngineError> {
        engine.check_admitted_session_origin(&admission, session)?;
        let cancel = CancellationToken::new();
        let notification = cancel.clone();
        admission.on_revoked(move || notification.cancel())?;
        Ok(Self { admission, cancel })
    }
    pub(crate) fn check(&self, engine: &Engine, session: SessionId) -> Result<(), EngineError> {
        engine.check_admitted_session_origin(&self.admission, session)
    }
    /// Native acquisition additionally requires the actual successful admission
    /// to remain active in this Engine. Projection-only callers retain check().
    pub(crate) fn check_live(
        &self,
        engine: &Engine,
        session: SessionId,
    ) -> Result<(), EngineError> {
        engine.check_admitted_session(&self.admission, session)
    }
    pub(super) fn check_active(&self) -> Result<(), EngineError> {
        self.admission.check_active()
    }
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}
