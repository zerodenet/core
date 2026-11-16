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
                ResolvedLeafOutbound::Direct {
                    tag: None,
                    dial_policy: Default::default(),
                    dial_generation: 0,
                },
            ],
        },
        Some(IPPROTO_ICMP),
        RouteMode::Auto,
    );
    let mut candidates = blocked.into_candidates().into_iter();
    assert!(matches!(candidates.next(), Some(PacketRouteTarget::Block)));
    assert!(matches!(
        candidates.next(),
        Some(PacketRouteTarget::DirectEcho { .. })
    ));

    let direct = inventory.prepare_packet_route_target_with_mode(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
            tag: None,
            dial_policy: Default::default(),
            dial_generation: 0,
        }),
        Some(IPPROTO_ICMP),
        RouteMode::Auto,
    );
    assert!(matches!(direct, PacketRouteTarget::DirectEcho { .. }));

    let flow = inventory.prepare_packet_route_target_with_mode(
        &config,
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
            tag: None,
            dial_policy: Default::default(),
            dial_generation: 0,
        }),
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
            ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
                tag: None,
                dial_policy: Default::default(),
                dial_generation: 0,
            }),
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
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
            tag: None,
            dial_policy: Default::default(),
            dial_generation: 0,
        }),
        Some(IPPROTO_ICMP),
        RouteMode::Flow,
    );
    assert!(matches!(target, PacketRouteTarget::Unsupported));
}

#[test]
fn translated_packet_requires_an_explicit_adapter_and_never_uses_direct_echo() {
    let target = ProtocolInventory::default().prepare_packet_route_target_with_mode(
        &config(),
        ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
            tag: None,
            dial_policy: Default::default(),
            dial_generation: 0,
        }),
        Some(IPPROTO_ICMP),
        RouteMode::Translate,
    );
    assert!(matches!(target, PacketRouteTarget::Unsupported));
}

#[cfg(feature = "wireguard")]
#[path = "tests/endpoint.rs"]
mod endpoint;

#[test]
fn constrained_direct_never_uses_native_packet_or_echo() {
    let config = config();
    let inventory = ProtocolInventory::default();
    for family in [
        zero_traits::AddressFamily::OnlyIpv4,
        zero_traits::AddressFamily::OnlyIpv6,
    ] {
        let leaf = || {
            ResolvedOutbound::Single(ResolvedLeafOutbound::Direct {
                tag: Some("direct"),
                dial_policy: zero_traits::DialPolicy {
                    address_family: family,
                    ..Default::default()
                },
                dial_generation: 1,
            })
        };
        for mode in [
            RouteMode::Auto,
            RouteMode::Packet,
            RouteMode::Flow,
            RouteMode::Translate,
        ] {
            assert!(matches!(
                inventory.prepare_packet_route_target_with_mode(
                    &config,
                    leaf(),
                    Some(IPPROTO_ICMP),
                    mode
                ),
                PacketRouteTarget::Unsupported
            ));
        }
        assert!(matches!(
            inventory.prepare_packet_route_target_with_mode(
                &config,
                leaf(),
                Some(IPPROTO_TCP),
                RouteMode::Auto
            ),
            PacketRouteTarget::Flow
        ));
        assert!(matches!(
            inventory.prepare_packet_route_target_with_mode(
                &config,
                leaf(),
                Some(IPPROTO_TCP),
                RouteMode::Packet
            ),
            PacketRouteTarget::Unsupported
        ));
    }
}
