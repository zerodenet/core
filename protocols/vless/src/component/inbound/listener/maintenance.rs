//! Wrap the actual source lifecycle, never a new Scope or worker executor.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynResourceLifecycle, ResourceLifecycle},
    scope::TaskRegistrar,
    CloseReason,
};
pub(super) fn attach(
    mut instance: ComponentInstance,
    maintenance: crate::encryption::HostedMaintenance,
    tasks: TaskRegistrar,
) -> ComponentInstance {
    instance.lifecycle = erase_lifecycle(Owner {
        source: instance.lifecycle,
        maintenance,
        tasks,
    });
    instance
}
struct Owner {
    source: Box<dyn DynResourceLifecycle>,
    maintenance: crate::encryption::HostedMaintenance,
    tasks: TaskRegistrar,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), ChannelError> {
        self.maintenance.start(&self.tasks)?;
        self.source.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.maintenance.revoke();
        self.source.revoke(reason);
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        self.maintenance.revoke();
        // This is only the source's protocol/native report. The owning Host
        // independently consumes the original maintenance witness and Scope join.
        self.source.shutdown(end).await
    }
}
