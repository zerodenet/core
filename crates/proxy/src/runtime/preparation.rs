//! The one cold production Host, retained across pre-Engine URL rule loading.
use super::*;

/// Initial URL downloads and the eventual Proxy consume the same inventory,
/// catalog identities and original custody root. Construction performs no IO.
/// This value is moved into `Proxy::from_preparation` after bootstrap cleanup.
pub struct ProxyPreparation {
    pub(super) egress_interface: zero_platform_tokio::EgressInterfaceControl,
    pub(super) node_network: Arc<crate::component::background::BackgroundNetwork>,
    pub(super) background: Option<Arc<crate::component::background::BackgroundNetwork>>,
    pub(super) protocols: ProtocolInventory,
}
impl Default for ProxyPreparation {
    fn default() -> Self { Self::new() }
}
impl ProxyPreparation {
    pub fn new() -> Self { Self::with_background(false) }
    pub(super) fn with_background(native_background: bool) -> Self {
        let egress_interface = zero_platform_tokio::EgressInterfaceControl::default();
        let node_network = Arc::new(crate::component::background::BackgroundNetwork::new(
            egress_interface.clone(),
        ));
        let background = native_background.then(|| node_network.clone());
        let protocols = match &background {
            Some(network) => ProtocolInventory::with_background(network.clone()),
            None => ProtocolInventory::with_node_network(node_network.clone()),
        };
        Self { egress_interface, node_network, background, protocols }
    }
    pub fn egress_interface_control(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.egress_interface.clone()
    }
    /// Apply the loaded document before the first bootstrap native acquisition.
    /// A changed limit still refuses any retained original from an older attempt.
    pub fn configure_original_limit(
        &self,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<(), EngineError> { install_original_limit(&self.protocols, runtime) }
    pub fn application_network_originals(&self) -> Result<(
        zero_core::component::ComponentInstanceId,
        Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    ), EngineError> {
        Ok((self.protocols.allocate_resource_identity()?,
            self.protocols.runtime_host().original_budgets().carrier.clone()))
    }
}
pub(super) fn install_original_limit(
    protocols: &ProtocolInventory,
    runtime: &zero_config::RuntimeOptionsConfig,
) -> Result<(), EngineError> {
    let limit = std::num::NonZeroUsize::new(runtime.effective_carrier_max_total_originals())
        .filter(|limit| limit.get() <= 65536)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid carrier original limit"))?;
    protocols.runtime_host().original_budgets().carrier.install_after_retirement(limit)?;
    Ok(())
}
