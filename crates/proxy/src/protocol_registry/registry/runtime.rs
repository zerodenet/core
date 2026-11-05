use super::ProtocolRegistry;
use zero_config::RuntimeConfig;

impl ProtocolRegistry {
    pub(crate) fn endpoint_control_available(&self) -> bool {
        !self.endpoint_controllers.is_empty()
    }
    pub(crate) fn endpoint_control_supported(
        &self,
        binding: &zero_config::EndpointBindingConfig,
    ) -> bool {
        self.endpoint_controllers
            .iter()
            .any(|capability| capability.supports_endpoint_control(binding))
    }

    pub(crate) fn observe_endpoint(
        &self,
        binding: &zero_config::EndpointBindingConfig,
        config: &RuntimeConfig,
    ) -> Option<crate::protocol_registry::EndpointObservation> {
        self.endpoint_observers
            .iter()
            .find_map(|observer| observer.observe_endpoint(binding, config))
    }

    pub(crate) fn endpoint_live_direction_contraction(
        &self,
        binding: &zero_config::EndpointBindingConfig,
    ) -> zero_api::EndpointDirections {
        self.endpoint_controllers
            .iter()
            .find(|capability| capability.supports_endpoint_control(binding))
            .map(|capability| capability.live_direction_contraction(binding))
            .unwrap_or_default()
    }

    pub(crate) fn validate_runtime_candidate(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(), zero_engine::EngineError> {
        for entry in &self.entries {
            entry.support.validate_runtime_candidate(config)?;
        }
        Ok(())
    }
    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        for entry in &self.entries {
            entry.support.on_config_reloaded(config);
        }
    }
}
