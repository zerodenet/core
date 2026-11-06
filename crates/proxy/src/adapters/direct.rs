use zero_config::InboundConfig;
use zero_engine::EngineError;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput};
use crate::runtime::path::TcpPathCategory;

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

impl DirectAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Direct {
            tag,
            dial_policy,
            dial_generation,
        } = input
        else {
            return None;
        };
        #[cfg(feature = "udp-runtime")]
        let policy_tag = tag.map(str::to_owned);
        #[cfg(not(feature = "udp-runtime"))]
        let _ = dial_generation;
        let configured_tag = tag.map(str::to_owned);
        let tag = tag.unwrap_or("direct").to_owned();
        let tcp = std::sync::Arc::new(
            crate::runtime::tcp_dispatch::operation::DirectTcpConnectOperation {
                tag: tag.clone(),
                configured_tag,
                dial_policy: dial_policy.clone(),
            },
        );
        Some(OutboundLeafClaim {
            #[cfg(not(feature = "udp-runtime"))]
            _lifetime: std::marker::PhantomData,
            tcp_path: TcpPathCategory::Direct,
            tcp: Some(Ok(tcp)),
            tcp_relay: None,
            #[cfg(feature = "udp-runtime")]
            udp: Some(Ok(Box::new(
                crate::runtime::udp_dispatch::operation::DirectUdpFlowOperation {
                    tag,
                    policy_tag,
                    dial_policy: dial_policy.clone(),
                    dial_generation,
                },
            ))),
            #[cfg(feature = "udp-runtime")]
            udp_relay: None,
            #[cfg(feature = "udp-runtime")]
            #[cfg(feature = "raw-ip-runtime")]
            packet: self
                .packet
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .filter(|d| dial_policy == zero_traits::DialPolicy::default() && d.usable())
                .map(|d| crate::runtime::packet_route::PacketRouteOperations {
                    native: std::sync::Arc::new(
                        crate::runtime::packet_route::host::PreparedHostPacketOperation {
                            device: d.clone(),
                            dial_policy: dial_policy.clone(),
                        },
                    ),
                    translated: None,
                    datagram: None,
                    flow: None,
                    correlated_returns: false,
                }),
            #[cfg(feature = "udp-runtime")]
            packet_path: None,
        })
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
