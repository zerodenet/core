use zero_config::{InboundConfig, OutboundConfig};
use zero_engine::EngineError;

use super::ProtocolRegistry;

impl ProtocolRegistry {
    /// Validate that every inbound in the config has a compiled-in adapter.
    pub(crate) fn validate_inbounds(&self, configs: &[InboundConfig]) -> Result<(), EngineError> {
        for inbound in configs {
            if let Some(entry) = self
                .entries
                .iter()
                .find(|entry| entry.support.supports_inbound(&inbound.protocol))
            {
                entry.support.validate_inbound_config(inbound)?;
            } else {
                let name = self.inbound_protocol_label(&inbound.protocol);
                return Err(EngineError::CompiledFeatureDisabled {
                    kind: "inbound",
                    tag: inbound.tag.clone(),
                    protocol: name,
                    feature: self.inbound_protocol_feature_name(&inbound.protocol),
                });
            }
        }
        Ok(())
    }

    /// Validate that every outbound in the config has a compiled-in adapter.
    pub(crate) fn validate_outbounds(&self, configs: &[OutboundConfig]) -> Result<(), EngineError> {
        for outbound in configs {
            if !self.supports_outbound(&outbound.protocol) {
                let name = self.outbound_protocol_label(&outbound.protocol);
                return Err(EngineError::CompiledFeatureDisabled {
                    kind: "outbound",
                    tag: outbound.tag.clone(),
                    protocol: name,
                    feature: self.outbound_protocol_feature_name(&outbound.protocol),
                });
            }
        }
        Ok(())
    }
}
