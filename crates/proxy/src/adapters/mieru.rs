use zero_config::OutboundProtocolConfig;
use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

use crate::adapters::identity::NamedProtocolAdapter;

pub(crate) mod component;
#[cfg(feature = "mieru")]
pub(crate) mod inbound;

#[cfg(feature = "mieru")]
#[derive(Debug, Default)]
pub(crate) struct MieruAdapter {
    pub(crate) native_host: bool,
    pool: std::sync::Arc<::mieru::client::ClientPool>,
}

#[cfg(feature = "mieru")]
impl NamedProtocolAdapter for MieruAdapter {
    const PROTOCOL_NAME: &'static str = "mieru";
    const FEATURE_NAME: &'static str = "mieru";
    fn validate_runtime_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), zero_engine::EngineError> {
        let present = config
            .outbounds
            .iter()
            .any(|node| matches!(node.protocol, OutboundProtocolConfig::Mieru { .. }));
        let native = self.native_host && present && component::production_selected(Some(config));
        self.pool
            .validate_component_candidate(native, present && !native)
            .map_err(zero_engine::EngineError::Io)
    }
    fn on_config_reloaded(&self, _config: &zero_config::RuntimeConfig) {
        self.pool.clear_legacy();
    }
}

#[cfg(feature = "mieru")]
impl ProtocolMetadata for MieruAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ::mieru::MieruProtocol.descriptor()
    }
}
