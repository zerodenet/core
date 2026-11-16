#![cfg(feature = "wireguard")]
//! A bare registry has no Source provider and cannot resurrect private stacks.
#[test]
fn cold_wireguard_registry_does_not_materialize_private_packet_or_flow_executors() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"wg","protocol":{"type":"wireguard",
            "private_key":STANDARD.encode([1;32]),"addresses":["10.0.0.1/32"],
            "peers":[{"public_key":STANDARD.encode([2;32]),"endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"final":{"type":"route","outbound":"wg"}}
    }).to_string()).unwrap();
    let inventory = crate::inventory::ProtocolInventory::default();
    for mode in [
        zero_engine::RouteMode::Auto,
        zero_engine::RouteMode::Flow,
        zero_engine::RouteMode::Packet,
        zero_engine::RouteMode::Translate,
    ] {
        for protocol in [
            zero_stack::packet::IPPROTO_TCP,
            zero_stack::packet::IPPROTO_UDP,
            zero_stack::packet::IPPROTO_ICMP,
            zero_stack::packet::IPPROTO_ICMPV6,
        ] {
            assert!(matches!(
                inventory.prepare_packet_route_target_with_mode(
                    &config,
                    zero_engine::ResolvedOutbound::Single(
                        zero_engine::ResolvedLeafOutbound::Proxy {
                            identity: zero_engine::OutboundIdentity::from_config_index(0),
                        }
                    ),
                    Some(protocol),
                    mode
                ),
                crate::inventory::PacketRouteTarget::Unsupported
            ));
        }
    }
}
