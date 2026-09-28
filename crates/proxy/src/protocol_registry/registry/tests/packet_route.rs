#![cfg(feature = "wireguard")]

use zero_config::RuntimeConfig;
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound, RouteMode};
use zero_stack::packet::IPPROTO_TCP;

use crate::inventory::{PacketRouteTarget, ProtocolInventory};

#[test]
fn forced_flow_accepts_packet_outbound_with_executable_tcp_udp_conversions() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use zero_engine::OutboundIdentity;
    use zero_stack::packet::IPPROTO_UDP;

    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[],
            "outbounds":[{"tag":"wg", "protocol":{"type":"wireguard",
                "private_key":STANDARD.encode([1_u8;32]), "addresses":["10.222.0.2/32"],
                "peers":[{"public_key":STANDARD.encode([2_u8;32]), "endpoint":"127.0.0.1:51820",
                    "allowed_ips":["192.168.1.0/24"]}]}}],
            "route":{"rules":[], "final":{"type":"route", "outbound":"wg"}}
        })
        .to_string(),
    )
    .unwrap();
    let inventory = ProtocolInventory::default();
    for (mode, protocol) in [
        (RouteMode::Flow, IPPROTO_TCP),
        (RouteMode::Flow, IPPROTO_UDP),
        (RouteMode::Translate, IPPROTO_TCP),
        (RouteMode::Translate, IPPROTO_UDP),
    ] {
        let target = inventory.prepare_packet_route_target_with_mode(
            &config,
            ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy {
                identity: OutboundIdentity::from_config_index(0),
            }),
            Some(protocol),
            mode,
        );
        assert!(matches!(target, PacketRouteTarget::Flow));
    }
    for protocol in [
        zero_stack::packet::IPPROTO_ICMP,
        zero_stack::packet::IPPROTO_ICMPV6,
    ] {
        for mode in [
            RouteMode::Auto,
            RouteMode::Packet,
            RouteMode::Flow,
            RouteMode::Translate,
        ] {
            let target = inventory.prepare_packet_route_target_with_mode(
                &config,
                ResolvedOutbound::Single(ResolvedLeafOutbound::Proxy {
                    identity: OutboundIdentity::from_config_index(0),
                }),
                Some(protocol),
                mode,
            );
            match mode {
                RouteMode::Flow => assert!(matches!(target, PacketRouteTarget::Unsupported)),
                RouteMode::Translate => assert!(matches!(
                    target,
                    PacketRouteTarget::Packet {
                        translated: true,
                        ..
                    }
                )),
                _ => assert!(matches!(
                    target,
                    PacketRouteTarget::Packet {
                        translated: false,
                        ..
                    }
                )),
            }
        }
    }
}
