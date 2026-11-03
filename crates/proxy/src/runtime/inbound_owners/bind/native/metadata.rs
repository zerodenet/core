//! Reuse the canonical runtime original provider in the actual inbound Scope.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle},
    CloseReason,
};
pub(super) struct Resource(pub Arc<crate::component::instance::HostResources>);
impl ResourceLifecycle for Resource {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.shutdown_io(deadline).await
    }
}
