use crate::runtime::inbound_service::PreparedInboundServiceOperation;
use zero_config::OutboundConfig;
use zero_engine::EngineError;

/// Optional active ingress preparation, separate from passive listener binding.
pub(crate) trait InboundServiceCapability: Send + Sync {
    fn prepare_inbound_service(
        &self,
        outbound: &OutboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Option<Box<dyn PreparedInboundServiceOperation>>, EngineError>;
}
