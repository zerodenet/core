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
