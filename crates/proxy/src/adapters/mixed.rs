use zero_config::InboundConfig;
use zero_engine::EngineError;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::InboundListenerCapability;

#[cfg(feature = "mixed")]
mod inbound;

#[cfg(feature = "mixed")]
#[derive(Debug)]
pub(crate) struct MixedAdapter;

#[cfg(feature = "mixed")]
impl NamedProtocolAdapter for MixedAdapter {
    const PROTOCOL_NAME: &'static str = "mixed";
    const FEATURE_NAME: &'static str = "mixed";
    const HAS_OUTBOUND: bool = false;
}

#[cfg(feature = "mixed")]
impl InboundListenerCapability for MixedAdapter {
    fn prepare_inbound_listener(
        &self,
        inbound: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        self.prepare_inbound_listener_impl(inbound)
    }
}

#[cfg(feature = "mixed")]
impl ProtocolMetadata for MixedAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "mixed",
            feature: "mixed",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "kernel_builtin",
            inbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::supported(),
                ProtocolCapabilityState::supported(),
            ),
            outbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::unsupported(&[]),
                ProtocolCapabilityState::unsupported(&[]),
            ),
            transports: &["tcp"],
            mux: ProtocolCapabilityState::not_applicable(),
            limitations: &[],
        }
    }
}
