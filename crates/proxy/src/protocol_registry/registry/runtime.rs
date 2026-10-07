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

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn outbound_device_health(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        self.outbound_devices
            .iter()
            .flat_map(|capability| {
                let outbounds = config
                    .outbounds
                    .iter()
                    .filter(|outbound| capability.supports_outbound(&outbound.protocol))
                    .collect::<Vec<_>>();
                capability.outbound_device_health(&outbounds)
            })
            .collect()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn prepare_outbound_devices(
        &self,
        config: &RuntimeConfig,
        context: crate::protocol_registry::OutboundDevicePreparationContext,
        admission: zero_engine::EndpointAdmission<'_>,
    ) -> Result<
        Vec<Box<dyn crate::protocol_registry::PreparedOutboundDeviceState>>,
        zero_engine::EngineError,
    > {
        let mut prepared = Vec::with_capacity(self.outbound_devices.len());
        for capability in &self.outbound_devices {
            let outbounds = config
                .outbounds
                .iter()
                .filter(|outbound| {
                    admission.device_enabled(&outbound.tag)
                        && capability.supports_outbound(&outbound.protocol)
                })
                .collect::<Vec<_>>();
            prepared.push(
                capability
                    .prepare_outbound_devices(&outbounds, &config.inbounds, context.clone())
                    .await?,
            );
        }
        Ok(prepared)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn shutdown_outbound_devices(
        &self,
    ) -> Vec<crate::protocol_registry::OutboundDeviceCompletion> {
        self.outbound_devices
            .iter()
            .map(|capability| capability.shutdown_outbound_devices())
            .collect()
    }

    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        for entry in &self.entries {
            entry.support.on_config_reloaded(config);
        }
    }
}
