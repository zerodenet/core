use super::ProtocolRegistry;
use crate::protocol_registry::InboundServiceCapability;
use crate::runtime::inbound_service::PreparedInboundServiceOperation;
use std::sync::Arc;
use zero_config::RuntimeConfig;
use zero_engine::EngineError;

impl ProtocolRegistry {
    pub(crate) fn register_inbound_service(
        &mut self,
        capability: Arc<dyn InboundServiceCapability>,
    ) {
        self.services.push(capability);
    }
    pub(crate) fn prepare_inbound_services(
        &self,
        config: &RuntimeConfig,
    ) -> Result<Vec<Box<dyn PreparedInboundServiceOperation>>, EngineError> {
        let mut prepared = Vec::new();
        for capability in &self.services {
            for outbound in &config.outbounds {
                prepared.extend(capability.prepare_inbound_service(outbound, config.source_dir())?);
            }
        }
        Ok(prepared)
    }
}
