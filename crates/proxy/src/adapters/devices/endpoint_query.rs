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

impl EndpointDeviceRows {
    pub(crate) fn serialize_details<S: serde::Serializer>(
        &self,
        config: &RuntimeConfig,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        #[cfg(feature = "wireguard")]
        {
            serde::Serialize::serialize(
                &super::super::wireguard::endpoint_details::Details {
                    facts: &self.wireguard,
                    config,
                },
                serializer,
            )
        }
        #[cfg(not(feature = "wireguard"))]
        {
            let _ = (self, config, serializer);
            Err(serde::ser::Error::custom(
                "endpoint protocol has no registered detail observer",
            ))
        }
    }
}
