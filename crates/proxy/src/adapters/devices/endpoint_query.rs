//! Concrete compiled endpoint observation; no provider/executor/query SPI.
use super::Devices;
use zero_config::{EndpointBindingConfig, RuntimeConfig};
use zero_runtime::configuration::ControlBudgetPermit;
pub(crate) struct EndpointDeviceRows {
    pub(crate) state: zero_api::EndpointRuntimeState,
    pub(crate) health: zero_api::EndpointHealthState,
    #[cfg(feature = "wireguard")]
    pub(crate) wireguard: super::super::wireguard::EndpointObservationRows,
}
impl Devices {
    pub(crate) fn endpoint_observer_present(&self, binding: &EndpointBindingConfig) -> bool {
        #[cfg(feature = "wireguard")]
        if binding.protocol == "wireguard" {
            return self.wireguard.is_some();
        }
        let _ = binding;
        false
    }
    pub(crate) fn control_endpoint_rows(
        &self,
        binding: &EndpointBindingConfig,
        config: &RuntimeConfig,
        workspace: &mut ControlBudgetPermit,
    ) -> std::io::Result<Option<EndpointDeviceRows>> {
        #[cfg(feature = "wireguard")]
        if binding.protocol == "wireguard" {
            if let Some(owner) = &self.wireguard {
                let facts = owner.control_endpoint_rows(binding, config, workspace)?;
                return Ok(Some(EndpointDeviceRows {
                    state: facts.state,
                    health: facts.health,
                    wireguard: facts,
                }));
            }
        }
        let _ = (binding, config, workspace);
        Ok(None)
    }
}
