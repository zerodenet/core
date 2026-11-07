//! Same concrete roots and real registrar supplied by the existing host.
use super::*;
impl HostResources {
    pub(in crate::component) fn new_hosted_with_packet_source(
        instance: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
        tasks: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
        originals: Arc<crate::HostOriginalBudgets>,
        declared_failures: Arc<super::super::failures::DeclaredReceiver>,
        protocol_failures: Arc<super::super::failures::DeclaredReceiver>,
        source: Arc<std::sync::OnceLock<zero_platform_tokio::controlled_carrier::CarrierOrigin>>,
    ) -> Result<Arc<Self>, ChannelError> {
        let mut resources = Self::construct(
            instance.component,
            declarations,
            cancel,
            Some((instance, tasks.scope_identity())),
            Some(source),
            originals,
            Some(tasks),
            protocol_failures,
        )?;
        let cold = Arc::get_mut(&mut resources).expect("cold resources not published");
        cold.declared_failures = declared_failures;
        Ok(resources)
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
            Arc::new(super::super::failures::DeclaredReceiver::protocol()),
            Arc::default(),
        )
    }
}
