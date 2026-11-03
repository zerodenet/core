//! The actual native owner is retained even when a handshake is interrupted.
use std::sync::Arc;
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        admission::SessionOwner,
        component::ResourceAccess,
        lifecycle::{BuildError, CloseReport, ResourceLifecycle},
        scope::TaskRegistrar,
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};

pub(super) struct Owner {
    pub session: SessionOwner,
    pub carrier: Option<CarrierOwner>,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if let Some(owner) = &mut self.carrier {
            self.resources
                .register_controlled_carrier(0, owner.registration(), &self.tasks)?;
            owner.start(deadline).await?;
        }
        self.session.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
        if let Some(owner) = &self.carrier {
            owner.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = self.session.shutdown(deadline).await?;
        if let Some(owner) = &mut self.carrier {
            report.append(owner.shutdown(deadline).await?);
        }
        Ok(report)
    }
}
