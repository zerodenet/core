#[cfg(feature = "socks5")]
use zero_engine::OutboundIdentity;
use zero_engine::ResolvedLeafOutbound;

use crate::runtime::path::TcpPathCategory;

use super::fixtures::{compiled_in_outbound_leaves, outbound_leaf_name};

fn test_protocol_descriptor(
    protocol: &'static str,
    feature: &'static str,
) -> zero_traits::ProtocolCapabilityDescriptor {
    let unsupported = zero_traits::ProtocolCapabilityState::unsupported(&[]);
    zero_traits::ProtocolCapabilityDescriptor {
        protocol,
        feature,
        status: zero_traits::ProtocolCapabilityLevel::Experimental,
        compatibility_baseline: "test",
        inbound: zero_traits::ProtocolNetworkCapability::new(unsupported, unsupported),
        outbound: zero_traits::ProtocolNetworkCapability::new(unsupported, unsupported),
        transports: &[],
        mux: unsupported,
        limitations: &[],
    }
}

#[test]
fn compiled_in_outbound_leaf_variants_have_expected_protocol_owners() {
    let registry = crate::register::protocol_registry();

    for (config, leaf, expected_owners) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let owner_count = registry
            .entries
            .iter()
            .filter(|entry| entry.support.name() == protocol)
            .count();
        assert_eq!(
            owner_count, expected_owners,
            "{} outbound leaf should have {expected_owners} registered protocol owner(s)",
            protocol
        );

        let claimed = registry.claim_outbound_leaf(&config, leaf.clone());
        assert_eq!(
            claimed.as_ref().map(|claim| claim.has_tcp_capability()).ok(),
            Some(expected_owners == 1),
            "{} claimed outbound lookup should expose runtime facts and optional adapter with the same ownership policy",
            protocol
        );
    }
}

#[test]
fn block_outbound_leaf_is_kernel_fact_not_adapter_protocol() {
    let registry = crate::register::protocol_registry();
    let config = zero_config::RuntimeConfig::parse(
        r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#,
    )
    .expect("minimal config");
    let leaf = ResolvedLeafOutbound::Block {
        tag: Some("blocked"),
    };

    let owner_count = registry
        .entries
        .iter()
        .filter(|entry| entry.support.name() == "block")
        .count();
    assert_eq!(
        owner_count, 0,
        "block should not have a registered protocol owner"
    );

    let claimed = registry
        .claim_outbound_leaf(&config, leaf.clone())
        .expect("block should still expose claimed runtime facts");
    assert!(
        !claimed.has_tcp_capability(),
        "block should not expose an outbound adapter"
    );

    assert_eq!(claimed.runtime.tcp_path, TcpPathCategory::Block);
    #[cfg(feature = "udp-runtime")]
    assert_eq!(claimed.runtime.health_tag, None);
    assert_eq!(claimed.runtime.endpoint, None);
    assert_eq!(claimed.runtime.kernel_tag, Some("blocked".to_owned()));
}

#[cfg(feature = "udp-runtime")]
#[test]
fn udp_outbound_leaf_lookup_matches_tcp_claim_policy() {
    let registry = crate::register::protocol_registry();

    for (config, leaf, expected_claims) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let claimed = registry.claim_outbound_leaf(&config, leaf.clone());
        assert_eq!(
            claimed
                .as_ref()
                .map(|claim| claim.has_udp_flow_capability())
                .ok(),
            Some(expected_claims == 1),
            "{} claimed udp-flow lookup should follow the same claim policy as tcp outbound lookup",
            protocol
        );
    }
}

#[cfg(feature = "udp-runtime")]
#[test]
fn packet_path_leaf_lookup_matches_claim_time_packet_path_projection() {
    let registry = crate::register::protocol_registry();

    for (config, leaf, _) in compiled_in_outbound_leaves() {
        let protocol = outbound_leaf_name(&config, &leaf);
        let expected_packet_path = matches!(protocol, "socks5" | "hysteria2" | "shadowsocks");
        let claimed = registry.claim_outbound_leaf(&config, leaf.clone());
        assert_eq!(
            claimed
                .as_ref()
                .map(|claim| claim.has_udp_packet_path_capability())
                .ok(),
            Some(expected_packet_path),
            "{} claimed packet-path lookup should only expose adapters with packet-path claim-time projection",
            protocol
        );
    }
}

#[cfg(feature = "socks5")]
#[test]
fn registry_executes_adapter_claimed_tcp_leaf_operations() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
    use zero_core::Session;
    use zero_engine::EngineError;
    use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

    use crate::protocol_registry::TcpRuntimeServices;
    use crate::protocol_registry::{
        ClaimedTcpOutboundLeaf, ClaimedUdpFlowLeaf, InboundListenerCapability, OutboundLeafClaim,
        OutboundLeafInput, ProtocolSupportCapability, TcpOutboundCapability, UdpFlowCapability,
        UdpPacketPathCapability,
    };
    use crate::runtime::tcp_dispatch::operation::{
        PreparedTcpConnectOperation, PreparedTcpRelayOperation,
    };
    use crate::runtime::udp_dispatch::operation::{
        DirectUdpFlowOperation, PreparedUdpFlowOperation,
    };
    use crate::runtime::udp_dispatch::FlowFailure;
    use crate::transport::{TcpOutboundFailure, TcpRelayStream};

    struct FakeClaimedLeaf;

    impl<'a> ClaimedTcpOutboundLeaf<'a> for FakeClaimedLeaf {
        fn prepare_tcp_connect(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
            Ok(Box::new(
                crate::runtime::tcp_dispatch::operation::DirectTcpConnectOperation {
                    tag: "claimed".to_owned(),
                },
            ))
        }

        fn prepare_tcp_relay_hop(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedTcpRelayOperation + 'a>, EngineError> {
            Ok(Box::new(FakeRelayOperation))
        }
    }

    struct FakeClaimedUdpLeaf;

    impl<'a> ClaimedUdpFlowLeaf<'a> for FakeClaimedUdpLeaf {
        fn prepare_udp_flow(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
            Ok(Box::new(DirectUdpFlowOperation {
                tag: "claimed".to_owned(),
            }))
        }
    }

    struct FakeRelayOperation;

    impl PreparedTcpRelayOperation for FakeRelayOperation {
        fn execute<'a>(
            self: Box<Self>,
            _services: TcpRuntimeServices,
            stream: TcpRelayStream,
            _session: &'a Session,
        ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
        where
            Self: 'a,
        {
            Box::pin(async move { Ok(stream) })
        }
    }

    struct FakeClaimedAdapter;

    impl ProtocolMetadata for FakeClaimedAdapter {
        fn descriptor(&self) -> ProtocolCapabilityDescriptor {
            test_protocol_descriptor("socks5", "test")
        }
    }

    impl ProtocolSupportCapability for FakeClaimedAdapter {
        fn name(&self) -> &'static str {
            "socks5"
        }

        fn feature_name(&self) -> &'static str {
            "test"
        }

        fn supports_inbound(&self, _config: &InboundProtocolConfig) -> bool {
            false
        }

        fn supports_outbound(&self, _config: &OutboundProtocolConfig) -> bool {
            false
        }

        fn has_inbound(&self) -> bool {
            false
        }

        fn has_outbound(&self) -> bool {
            true
        }
    }

    impl InboundListenerCapability for FakeClaimedAdapter {}

    impl FakeClaimedAdapter {
        fn claim_outbound_leaf_impl<'a>(
            &self,
            input: OutboundLeafInput<'a>,
        ) -> Option<OutboundLeafClaim<'a>> {
            let OutboundLeafInput::Proxy { outbound, .. } = input else {
                return None;
            };
            let OutboundProtocolConfig::Socks5 { .. } = &outbound.protocol else {
                return None;
            };
            let tcp: Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> = Box::new(FakeClaimedLeaf);
            Some(OutboundLeafClaim {
                tcp_path: TcpPathCategory::Tunnel,
                tcp,
                udp: Some(Box::new(FakeClaimedUdpLeaf)),
                packet_path: None,
            })
        }
    }

    impl TcpOutboundCapability for FakeClaimedAdapter {}

    impl UdpFlowCapability for FakeClaimedAdapter {}
    impl UdpPacketPathCapability for FakeClaimedAdapter {}

    let mut registry = super::super::ProtocolRegistry::default();
    registry.register_capability(
        Arc::new(FakeClaimedAdapter),
        FakeClaimedAdapter::claim_outbound_leaf_impl,
    );

    let config = socks5_test_config("claimed");
    let claimed = registry
        .claim_outbound_leaf(&config, proxy_test_leaf("claimed"))
        .expect("claim-time tcp leaf");

    match claimed.prepare_tcp_connect(None) {
        Ok(_) => {}
        Err(_) => panic!("claimed tcp connect operation"),
    }
    match claimed.prepare_tcp_relay_hop(None) {
        Ok(_) => {}
        Err(_) => panic!("claimed tcp relay operation"),
    };
}

#[cfg(feature = "socks5")]
#[test]
fn registry_executes_adapter_claimed_udp_leaf_operations() {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
    use zero_core::Session;
    use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

    use crate::protocol_registry::{
        ClaimedTcpOutboundLeaf, ClaimedUdpFlowLeaf, InboundListenerCapability, OutboundLeafClaim,
        OutboundLeafInput, ProtocolSupportCapability, TcpOutboundCapability, UdpFlowCapability,
        UdpPacketPathCapability,
    };
    use crate::runtime::tcp_dispatch::operation::{
        DirectTcpConnectOperation, PreparedTcpConnectOperation,
    };
    use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
    use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;
    use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
    use crate::transport::{RelayCarrier, TcpOutboundFailure, TcpRelayStream};

    struct FakeClaimedUdpLeaf;
    struct FakePreparedUdpRelay;

    impl<'a> PreparedUdpRelayOperation<'a> for FakePreparedUdpRelay {
        fn bind_final_hop(
            self: Box<Self>,
            _carrier: RelayCarrier,
        ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
            Ok(Box::new(FakeUdpFlowOperation))
        }
    }

    impl<'a> ClaimedUdpFlowLeaf<'a> for FakeClaimedUdpLeaf {
        fn prepare_udp_flow(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
            Ok(Box::new(FakeUdpFlowOperation))
        }

        fn prepare_udp_relay(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedUdpRelayOperation<'a> + 'a>, FlowFailure> {
            Ok(Box::new(FakePreparedUdpRelay))
        }
    }

    struct FakeClaimedTcpLeaf;

    impl<'a> ClaimedTcpOutboundLeaf<'a> for FakeClaimedTcpLeaf {
        fn prepare_tcp_connect(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
            Ok(Box::new(DirectTcpConnectOperation {
                tag: "fake-claimed-udp".to_owned(),
            }))
        }
    }

    struct FakeUdpFlowOperation;

    impl PreparedUdpFlowOperation for FakeUdpFlowOperation {
        fn execute<'a>(
            self: Box<Self>,
            _dispatch: &'a mut UdpDispatch,
            _ctx: crate::protocol_registry::UdpAdapterContext<'a>,
            _session: &'a Session,
            _payload: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
        where
            Self: 'a,
        {
            Box::pin(async move {
                Ok(FlowStartResult::Blocked {
                    tag: "claimed-udp".to_owned(),
                })
            })
        }
    }

    struct FakeClaimedUdpAdapter;

    impl ProtocolMetadata for FakeClaimedUdpAdapter {
        fn descriptor(&self) -> ProtocolCapabilityDescriptor {
            test_protocol_descriptor("socks5", "test")
        }
    }

    impl ProtocolSupportCapability for FakeClaimedUdpAdapter {
        fn name(&self) -> &'static str {
            "socks5"
        }

        fn feature_name(&self) -> &'static str {
            "test"
        }

        fn supports_inbound(&self, _config: &InboundProtocolConfig) -> bool {
            false
        }

        fn supports_outbound(&self, _config: &OutboundProtocolConfig) -> bool {
            false
        }

        fn has_inbound(&self) -> bool {
            false
        }

        fn has_outbound(&self) -> bool {
            true
        }
    }

    impl InboundListenerCapability for FakeClaimedUdpAdapter {}

    impl FakeClaimedUdpAdapter {
        fn claim_outbound_leaf_impl<'a>(
            &self,
            input: OutboundLeafInput<'a>,
        ) -> Option<OutboundLeafClaim<'a>> {
            let OutboundLeafInput::Proxy { outbound, .. } = input else {
                return None;
            };
            let OutboundProtocolConfig::Socks5 { .. } = &outbound.protocol else {
                return None;
            };
            let tcp: Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> = Box::new(FakeClaimedTcpLeaf);
            Some(OutboundLeafClaim {
                tcp_path: TcpPathCategory::Tunnel,
                tcp,
                udp: Some(Box::new(FakeClaimedUdpLeaf)),
                packet_path: None,
            })
        }
    }

    impl TcpOutboundCapability for FakeClaimedUdpAdapter {}

    impl UdpFlowCapability for FakeClaimedUdpAdapter {}

    impl UdpPacketPathCapability for FakeClaimedUdpAdapter {}

    let mut registry = super::super::ProtocolRegistry::default();
    registry.register_capability(
        Arc::new(FakeClaimedUdpAdapter),
        FakeClaimedUdpAdapter::claim_outbound_leaf_impl,
    );

    let config = socks5_test_config("claimed-udp");
    let claimed = registry
        .claim_outbound_leaf(&config, proxy_test_leaf("claimed-udp"))
        .expect("claim-time udp leaf");

    match claimed.prepare_udp_flow(None) {
        Ok(_) => {}
        Err(_) => panic!("claimed udp flow operation"),
    }

    let (stream, _peer) = tokio::io::duplex(64);
    let prepared_relay = match claimed.prepare_udp_relay(None) {
        Ok(prepared) => prepared,
        Err(_) => panic!("prepare claimed UDP relay"),
    };
    match prepared_relay.bind_final_hop(RelayCarrier {
        stream: TcpRelayStream::new(stream),
        server: "claimed-udp.test".to_owned(),
        port: 8443,
    }) {
        Ok(_) => {}
        Err(_) => panic!("claimed udp relay-final operation"),
    };
}

#[cfg(feature = "socks5")]
#[test]
fn registry_executes_adapter_claimed_udp_packet_path_operations() {
    use std::sync::Arc;

    use zero_config::{InboundProtocolConfig, OutboundProtocolConfig};
    use zero_traits::{ProtocolCapabilityDescriptor, ProtocolMetadata};

    use crate::protocol_registry::{
        ClaimedTcpOutboundLeaf, ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf,
        InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability,
        TcpOutboundCapability, UdpFlowCapability, UdpPacketPathCapability,
    };
    use crate::runtime::tcp_dispatch::operation::{
        DirectTcpConnectOperation, PreparedTcpConnectOperation,
    };
    use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
    use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
    use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
    use crate::transport::RelayCarrier;
    use crate::transport::TcpOutboundFailure;

    struct FakeClaimedUdpPacketPathLeaf;

    struct FakeClaimedTcpLeaf;

    impl<'a> ClaimedTcpOutboundLeaf<'a> for FakeClaimedTcpLeaf {
        fn prepare_tcp_connect(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
            Ok(Box::new(DirectTcpConnectOperation {
                tag: "fake-claimed-udp-packet-path".to_owned(),
            }))
        }
    }

    struct FakeClaimedUdpLeaf;
    struct FakePreparedUdpRelay;

    impl<'a> crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation<'a>
        for FakePreparedUdpRelay
    {
        fn bind_final_hop(
            self: Box<Self>,
            _carrier: RelayCarrier,
        ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
            Ok(Box::new(FakeClaimedUdpFlowOperation))
        }
    }

    impl<'a> ClaimedUdpFlowLeaf<'a> for FakeClaimedUdpLeaf {
        fn prepare_udp_flow(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
            Ok(Box::new(FakeClaimedUdpFlowOperation))
        }

        fn prepare_udp_relay(
            &self,
            _source_dir: Option<&std::path::Path>,
        ) -> Result<
            Box<dyn crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation<'a> + 'a>,
            FlowFailure,
        > {
            Ok(Box::new(FakePreparedUdpRelay))
        }
    }

    impl<'a> ClaimedUdpPacketPathLeaf<'a> for FakeClaimedUdpPacketPathLeaf {
        fn prepare_udp_packet_path(&self) -> Option<Box<dyn PreparedUdpPacketPathOperation + 'a>> {
            Some(Box::new(FakeUdpPacketPathOperation))
        }
    }

    struct FakeUdpPacketPathOperation;

    impl PreparedUdpPacketPathOperation for FakeUdpPacketPathOperation {}

    struct FakeClaimedUdpFlowOperation;

    impl PreparedUdpFlowOperation for FakeClaimedUdpFlowOperation {
        fn execute<'a>(
            self: Box<Self>,
            _dispatch: &'a mut UdpDispatch,
            _ctx: crate::protocol_registry::UdpAdapterContext<'a>,
            _session: &'a zero_core::Session,
            _payload: &'a [u8],
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>,
        >
        where
            Self: 'a,
        {
            Box::pin(async move {
                Ok(FlowStartResult::Blocked {
                    tag: "fake-claimed-udp-packet-path".to_owned(),
                })
            })
        }
    }

    struct FakeClaimedUdpPacketPathAdapter;

    impl ProtocolMetadata for FakeClaimedUdpPacketPathAdapter {
        fn descriptor(&self) -> ProtocolCapabilityDescriptor {
            test_protocol_descriptor("socks5", "test")
        }
    }

    impl ProtocolSupportCapability for FakeClaimedUdpPacketPathAdapter {
        fn name(&self) -> &'static str {
            "socks5"
        }

        fn feature_name(&self) -> &'static str {
            "test"
        }

        fn supports_inbound(&self, _config: &InboundProtocolConfig) -> bool {
            false
        }

        fn supports_outbound(&self, _config: &OutboundProtocolConfig) -> bool {
            false
        }

        fn has_inbound(&self) -> bool {
            false
        }

        fn has_outbound(&self) -> bool {
            true
        }
    }

    impl InboundListenerCapability for FakeClaimedUdpPacketPathAdapter {}

    impl FakeClaimedUdpPacketPathAdapter {
        fn claim_outbound_leaf_impl<'a>(
            &self,
            input: OutboundLeafInput<'a>,
        ) -> Option<OutboundLeafClaim<'a>> {
            let OutboundLeafInput::Proxy { outbound, .. } = input else {
                return None;
            };
            let OutboundProtocolConfig::Socks5 { .. } = &outbound.protocol else {
                return None;
            };
            let tcp: Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> = Box::new(FakeClaimedTcpLeaf);
            Some(OutboundLeafClaim {
                tcp_path: TcpPathCategory::Tunnel,
                tcp,
                udp: Some(Box::new(FakeClaimedUdpLeaf)),
                packet_path: Some(Box::new(FakeClaimedUdpPacketPathLeaf)),
            })
        }
    }

    impl TcpOutboundCapability for FakeClaimedUdpPacketPathAdapter {}

    impl UdpFlowCapability for FakeClaimedUdpPacketPathAdapter {}

    impl UdpPacketPathCapability for FakeClaimedUdpPacketPathAdapter {}

    let mut registry = super::super::ProtocolRegistry::default();
    registry.register_capability(
        Arc::new(FakeClaimedUdpPacketPathAdapter),
        FakeClaimedUdpPacketPathAdapter::claim_outbound_leaf_impl,
    );

    let config = socks5_test_config("claimed-udp-packet-path");
    let claimed = registry
        .claim_outbound_leaf(&config, proxy_test_leaf("claimed-udp-packet-path"))
        .expect("claim-time udp packet-path leaf");

    assert!(
        claimed.prepare_udp_packet_path().is_some(),
        "claimed packet-path leaf should prepare without falling back to raw-leaf callbacks"
    );
}

#[cfg(feature = "socks5")]
fn socks5_test_config(tag: &str) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&format!(
        r#"{{
            "outbounds": [{{
                "tag": "{tag}",
                "protocol": {{
                    "type": "socks5",
                    "server": "127.0.0.1",
                    "port": 1080
                }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("socks5 test config")
}

#[cfg(feature = "socks5")]
fn proxy_test_leaf(tag: &'static str) -> ResolvedLeafOutbound<'static> {
    let _ = tag;
    ResolvedLeafOutbound::Proxy {
        identity: OutboundIdentity::from_config_index(0),
    }
}
