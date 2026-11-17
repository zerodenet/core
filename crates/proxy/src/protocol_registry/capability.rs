use zero_config::{InboundConfig, InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_engine::EngineError;
use zero_traits::ProtocolMetadata;

pub(crate) trait ProtocolSupportCapability: ProtocolMetadata + Send + Sync {
    fn name(&self) -> &'static str;
    fn feature_name(&self) -> &'static str;
    fn supports_inbound(&self, config: &InboundProtocolConfig) -> bool;
    fn supports_outbound(&self, config: &OutboundProtocolConfig) -> bool;
    fn has_inbound(&self) -> bool;
    fn has_outbound(&self) -> bool;

    fn validate_inbound_config(&self, _config: &InboundConfig) -> Result<(), EngineError> {
        Ok(())
    }

    fn validate_runtime_candidate(&self, _config: &RuntimeConfig) -> Result<(), EngineError> {
        Ok(())
    }
    fn on_config_reloaded(&self, _config: &RuntimeConfig) {}
}
