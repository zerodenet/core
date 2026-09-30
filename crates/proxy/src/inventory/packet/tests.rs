use zero_config::RuntimeConfig;
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound, RouteMode};
use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_TCP};

use super::{PacketRouteTarget, ProtocolInventory};

fn config() -> RuntimeConfig {
    RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{"tag": "direct", "protocol": {"type": "direct"}}],
            "route": {"rules": [], "final": {"type": "direct"}}
        }"#,
    )
    .unwrap()
}

#[test]
fn automatic_path_keeps_outbound_candidate_order() {
    let config = config();
    let inventory = ProtocolInventory::default();
    let blocked = inventory.prepare_packet_route_target_with_mode(
        &config,
        ResolvedOutbound::Fallback {
            candidates: vec![
                ResolvedLeafOutbound::Block { tag: None },
                ResolvedLeafOutbound::Direct { tag: None },
            ],
        },
        Some(IPPROTO_ICMP),
        RouteMode::Auto,
    );
    let mut candidates = blocked.into_candidates().into_iter();
    assert!(matches!(candidates.next(), Some(PacketRouteTarget::Block)));
    assert!(matches!(
        candidates.next(),
        Some(PacketRouteTarget::DirectEcho)
    ));

    let direct = inventory.prepare_packet_route_target_with_mode(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_ICMP),
        RouteMode::Auto,
    );
    assert!(matches!(direct, PacketRouteTarget::DirectEcho));

    let flow = inventory.prepare_packet_route_target_with_mode(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_TCP),
        RouteMode::Auto,
    );
    assert!(matches!(flow, PacketRouteTarget::Flow));
}

#[test]
fn forced_packet_does_not_reinterpret_direct_flow_or_echo_as_packet_sink() {
    let config = config();
    let inventory = ProtocolInventory::default();
    for protocol in [IPPROTO_TCP, IPPROTO_ICMP] {
        let target = inventory.prepare_packet_route_target_with_mode(
            &config,
            ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
            Some(protocol),
            RouteMode::Packet,
        );
        assert!(matches!(target, PacketRouteTarget::Unsupported));
    }
}

#[test]
fn forced_flow_rejects_icmp_without_a_packet_sink() {
    let target = ProtocolInventory::default().prepare_packet_route_target_with_mode(
        &config(),
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_ICMP),
        RouteMode::Flow,
    );
    assert!(matches!(target, PacketRouteTarget::Unsupported));
}

#[test]
fn translated_packet_requires_an_explicit_adapter_and_never_uses_direct_echo() {
    let target = ProtocolInventory::default().prepare_packet_route_target_with_mode(
        &config(),
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
        Some(IPPROTO_ICMP),
        RouteMode::Translate,
    );
    assert!(matches!(target, PacketRouteTarget::Unsupported));
}

mod endpoint {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde_json::json;
    use zero_engine::OutboundIdentity;
    use zero_stack::packet::IPPROTO_UDP;

    use super::*;

    fn target(inbound: bool, protocol: u8, mode: RouteMode) -> PacketRouteTarget {
        let config = RuntimeConfig::parse(
            &json!({"endpoints":[{"tag":"wg", "directions":{"inbound":inbound,"outbound":true},
                "listen":{"address":"127.0.0.1","port":51820}, "protocol":{
                    "type":"wireguard", "private_key":STANDARD.encode([1;32]), "addresses":["10.0.0.1/32"],
                    "peers":[{"public_key":STANDARD.encode([2;32]), "endpoint":"127.0.0.1:51821", "allowed_ips":["10.0.0.0/24"]}]}}],
                "route":{"rules":[],"final":{"type":"route","outbound":"wg"}}}).to_string(),
        ).unwrap();
        ProtocolInventory::default().prepare_packet_route_target_with_mode(
            &config,
            ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy {
                identity: OutboundIdentity::from_config_index(0),
            }),
            Some(protocol),
            mode,
        )
    }

    #[test]
    fn outbound_only_packet_ingress_uses_correlated_flows_and_rejects_unsafe_native_returns() {
        if !ProtocolInventory::default()
            .supported_outbounds()
            .contains(&"wireguard")
        {
            assert!(matches!(
                target(false, IPPROTO_TCP, RouteMode::Auto),
                PacketRouteTarget::Unsupported
            ));
            return;
        }
        for protocol in [IPPROTO_TCP, IPPROTO_UDP] {
            assert!(matches!(
                target(false, protocol, RouteMode::Auto),
                PacketRouteTarget::Flow
            ));
            assert!(matches!(
                target(false, protocol, RouteMode::Packet),
                PacketRouteTarget::Unsupported
            ));
        }
        assert!(matches!(
            target(false, IPPROTO_ICMP, RouteMode::Auto),
            PacketRouteTarget::Unsupported
        ));
        assert!(matches!(
            target(false, IPPROTO_ICMP, RouteMode::Translate),
            PacketRouteTarget::Packet {
                translated: true,
                ..
            }
        ));
    }

    #[test]
    fn bidirectional_endpoint_preserves_native_packet_selection() {
        if !ProtocolInventory::default()
            .supported_outbounds()
            .contains(&"wireguard")
        {
            assert!(matches!(
                target(true, IPPROTO_TCP, RouteMode::Auto),
                PacketRouteTarget::Unsupported
            ));
            return;
        }
        for protocol in [IPPROTO_TCP, IPPROTO_UDP, IPPROTO_ICMP] {
            assert!(matches!(
                target(true, protocol, RouteMode::Auto),
                PacketRouteTarget::Packet {
                    translated: false,
                    ..
                }
            ));
        }
    }
}
