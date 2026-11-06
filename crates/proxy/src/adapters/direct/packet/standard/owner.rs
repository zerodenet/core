use super::*;
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskRegistrar,
    CloseReason,
};
pub(super) struct Owner {
    pub(super) shared: Arc<Shared>,
    pub(super) options: Arc<Options>,
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) tasks: TaskRegistrar,
    pub(super) instance: ComponentInstanceId,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        if self.shared.device.get().is_some() {
            return Err(error(
                Reason::StalePlan,
                "Direct native driver was already attempted",
            ));
        }
        let (device, mut input) = HostPacketDevice::prepare_scoped(
            self.options.config.clone(),
            self.options.mtu,
            self.instance,
            &self.tasks,
            &self.options.stack,
        )?;
        self.shared.device.set(device.clone()).map_err(|_| {
            error(
                Reason::StalePlan,
                "same Direct native driver is already installed",
            )
        })?;
        input.wait_input_pending(deadline).await?;
        let grant = tokio::time::timeout_at(
            deadline,
            self.resources
                .open_packet_device(NATIVE_NAME.into(), input.witness()),
        )
        .await
        .map_err(|source| {
            error(
                Reason::Timeout,
                "Direct original native acquisition remains owned",
            )
            .with_source(source)
        })??;
        input.install(grant)?;
        tokio::time::timeout_at(deadline, device.wait_native_view())
            .await
            .map_err(|source| {
                error(Reason::Timeout, "Direct native view transfer pending").with_source(source)
            })?
            .map_err(|source| {
                error(
                    Reason::Io,
                    "Direct original driver did not take its unique view",
                )
                .with_source(source)
            })
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(device) = self.shared.device.get() {
            device.close();
        }
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = CloseReport::default();
        if let Some(device) = self.shared.device.get() {
            match tokio::time::timeout_at(deadline, device.join_scoped_driver()).await {
                Err(_) => {
                    report.unfinished.push(Unfinished::Resource(
                        "same Direct Packet driver Scope has not joined".into(),
                    ));
                    return Ok(report);
                }
                Ok(Err(source)) => {
                    tracing::debug!(%source,"original Direct driver joined with retained Scope fault")
                }
                Ok(Ok(())) => {}
            }
            if !device.logical_sessions_closed() {
                report.unfinished.push(Unfinished::Resource(
                    "same Direct Packet logical views/control remain retained".into(),
                ));
            }
        }
        Ok(report) // Native close is exclusively the same HostResources provider.
    }
}
