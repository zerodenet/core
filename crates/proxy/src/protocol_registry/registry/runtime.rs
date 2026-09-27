use super::ProtocolRegistry;
use zero_config::RuntimeConfig;

impl ProtocolRegistry {
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
    ) -> Result<
        Vec<Box<dyn crate::protocol_registry::PreparedOutboundDeviceState>>,
        zero_engine::EngineError,
    > {
        let mut prepared = Vec::with_capacity(self.outbound_devices.len());
        for capability in &self.outbound_devices {
            let outbounds = config
                .outbounds
                .iter()
                .filter(|outbound| capability.supports_outbound(&outbound.protocol))
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
    pub(crate) fn shutdown_outbound_devices(&self) {
        for capability in &self.outbound_devices {
            capability.shutdown_outbound_devices();
        }
    }

    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        for entry in &self.entries {
            entry.support.on_config_reloaded(config);
        }
    }
}
