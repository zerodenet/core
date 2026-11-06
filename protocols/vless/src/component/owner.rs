//! Same protocol lifecycle, with the actual File material stage before Ready.
use super::*;
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{CloseReport, ResourceLifecycle},
    CancellationToken, CloseReason,
};
pub(super) struct Owner {
    pub(super) session: SessionOwner,
    pub(super) material: Option<Arc<material::Material>>,
    pub(super) instance: zero_core::component::ComponentInstanceId,
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) cancel: CancellationToken,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), ChannelError> {
        if let Some(material) = &self.material {
            material
                .start(self.resources.as_ref(), self.instance, &self.cancel, end)
                .await?;
        }
        self.session.start(end).await?;
        if let Some(material) = &self.material {
            material.record(self.instance)?;
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        self.session.shutdown(end).await
    }
}
