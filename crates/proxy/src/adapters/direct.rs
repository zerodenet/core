use zero_config::InboundConfig;
use zero_engine::EngineError;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, TcpOutboundCapability,
};
#[cfg(feature = "udp-runtime")]
use crate::protocol_registry::{UdpFlowCapability, UdpPacketPathCapability};
use crate::runtime::path::TcpPathCategory;

mod inbound;
mod tcp;
#[cfg(feature = "udp-runtime")]
mod udp;

// Direct inbound is always available (no feature gate).
#[derive(Debug)]
pub(crate) struct DirectAdapter;

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

impl DirectAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Direct { tag } = input else {
            return None;
        };
        let tag = tag.unwrap_or("direct").to_owned();
        let tcp = self.claim_tcp_outbound_leaf_impl(tag.clone());
        Some(OutboundLeafClaim {
            tcp_path: TcpPathCategory::Direct,
            tcp,
            #[cfg(feature = "udp-runtime")]
            udp: Some(self.claim_udp_flow_leaf_impl(tag)),
            #[cfg(feature = "udp-runtime")]
            packet_path: None,
        })
    }
}

#[cfg(feature = "udp-runtime")]
impl UdpFlowCapability for DirectAdapter {}

#[cfg(feature = "udp-runtime")]
impl UdpPacketPathCapability for DirectAdapter {}

#[async_trait::async_trait]
impl InboundListenerCapability for DirectAdapter {
    async fn bind_inbound(
        &self,
        inbound: &InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
        self.bind_inbound_impl(inbound).await
    }

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

impl TcpOutboundCapability for DirectAdapter {}

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
            limitations: &[],
        }
    }
}
