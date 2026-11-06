//! Same concrete roots and real registrar supplied by the existing host.
use super::*;
impl HostResources {
    pub(in crate::component) fn new_hosted_with_packet_source(
        instance: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        tasks: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        originals: Arc<crate::HostOriginalBudgets>,
        source: Arc<
            std::sync::OnceLock<
                zero_platform_tokio::controlled_carrier::CarrierOrigin,
            >,
        >,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(
            instance.component,
            declarations,
            cancel,
            Some((instance, tasks.scope_identity())),
            Some(source),
            originals,
            Some(tasks),
        )
    }
    /// Concrete hosted construction; no identity-to-registrar reconstruction.
    pub fn new_hosted(
        instance: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        tasks: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        originals: Arc<crate::HostOriginalBudgets>,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::new_hosted_with_packet_source(
            instance,
            declarations,
            cancel,
            tasks,
            originals,
            Arc::default(),
        )
    }
}
