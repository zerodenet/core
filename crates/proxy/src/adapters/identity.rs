//! Adapter identity and shared support capability implementation.

use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_traits::ProtocolMetadata;

use crate::protocol_registry::ProtocolSupportCapability;

pub(crate) trait NamedProtocolAdapter: ProtocolMetadata + Send + Sync {
    const PROTOCOL_NAME: &'static str;
    const FEATURE_NAME: &'static str;
    const HAS_INBOUND: bool = true;
    const HAS_OUTBOUND: bool = true;

    fn on_config_reloaded(&self, _config: &RuntimeConfig) {}
}

impl<T> ProtocolSupportCapability for T
where
    T: NamedProtocolAdapter,
{
    fn name(&self) -> &'static str {
        T::PROTOCOL_NAME
    }

    fn feature_name(&self) -> &'static str {
        T::FEATURE_NAME
    }

    fn supports_inbound(&self, config: &InboundProtocolConfig) -> bool {
        T::HAS_INBOUND && config.protocol_name() == T::PROTOCOL_NAME
    }

    fn supports_outbound(&self, config: &OutboundProtocolConfig) -> bool {
        T::HAS_OUTBOUND && config.protocol_name() == T::PROTOCOL_NAME
    }

    fn has_inbound(&self) -> bool {
        T::HAS_INBOUND
    }

    fn has_outbound(&self) -> bool {
        T::HAS_OUTBOUND
    }

    fn on_config_reloaded(&self, config: &RuntimeConfig) {
        NamedProtocolAdapter::on_config_reloaded(self, config);
    }
}
