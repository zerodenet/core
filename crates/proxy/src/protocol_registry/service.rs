use crate::runtime::inbound_service::NativeServiceOperation;
use zero_config::OutboundConfig;
use zero_engine::EngineError;

/// Optional active ingress preparation, separate from passive listener binding.
pub(crate) trait InboundServiceCapability: Send + Sync {
    fn prepare_inbound_service(
        &self,
        outbound: &OutboundConfig,
    ) -> Result<Option<NativeServiceOperation>, EngineError>;
}
