use zero_config::InboundConfig;
use zero_engine::EngineError;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;

mod datagram;
pub(crate) mod factory;
pub(crate) mod inbound;

// Direct inbound is always available (no feature gate).
#[derive(Default)]
pub(crate) struct DirectAdapter {
    #[cfg(feature = "raw-ip-runtime")]
    packet_candidate: std::sync::Mutex<Option<packet::standard::Candidate>>,
    #[cfg(feature = "raw-ip-runtime")]
    packet: std::sync::Arc<
        std::sync::Mutex<
            Option<std::sync::Arc<crate::runtime::packet_route::host::HostPacketDevice>>,
        >,
    >,
}
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod packet;

impl NamedProtocolAdapter for DirectAdapter {
    const PROTOCOL_NAME: &'static str = "direct";
    const FEATURE_NAME: &'static str = "core";
    const HAS_OUTBOUND: bool = false;

    fn validate_inbound_config(&self, config: &InboundConfig) -> Result<(), EngineError> {
        if config.udp.enabled && !cfg!(feature = "managed-datagram-runtime") {
            return Err(EngineError::CompiledFeatureDisabled {
                kind: "inbound UDP",
                tag: config.tag.clone(),
                protocol: "direct",
                feature: "managed-datagram-runtime",
            });
        }
        Ok(())
    }
}

impl ProtocolMetadata for DirectAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "direct",
            feature: "core",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "kernel_builtin",
            inbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::supported(),
                if cfg!(feature = "managed-datagram-runtime") {
                    ProtocolCapabilityState::supported()
                } else {
                    ProtocolCapabilityState::unsupported(&["managed-datagram-runtime"])
                },
            ),
            outbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::supported(),
                ProtocolCapabilityState::supported(),
            ),
            transports: &["tcp", "udp"],
            mux: ProtocolCapabilityState::not_applicable(),
            limitations: &["nondefault_dial_tcp_udp_only"],
        }
    }
}
