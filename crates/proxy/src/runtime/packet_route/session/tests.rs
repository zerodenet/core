use super::{PacketPlane, PacketSessionPins};

#[test]
fn new_tcp_connection_can_use_new_plane_while_existing_connection_stays_pinned() {
    let mut first = vec![0_u8; 40];
    first[0] = 0x45;
    first[2..4].copy_from_slice(&40_u16.to_be_bytes());
    first[9] = zero_stack::packet::IPPROTO_TCP;
    first[12..16].copy_from_slice(&[10, 0, 0, 2]);
    first[16..20].copy_from_slice(&[192, 168, 1, 119]);
    first[20..22].copy_from_slice(&40000_u16.to_be_bytes());
    first[22..24].copy_from_slice(&443_u16.to_be_bytes());
    first[32] = 0x50;
    let mut pins = PacketSessionPins::default();
    assert!(pins.record(&first, PacketPlane::Packet("wg".into())));
    assert!(!pins.permits(&first, &PacketPlane::Flow));
    let mut second = first.clone();
    second[20..22].copy_from_slice(&40001_u16.to_be_bytes());
    assert!(pins.permits(&second, &PacketPlane::Flow));
    assert!(pins.record(&second, PacketPlane::Flow));
    assert!(pins.permits(&first, &PacketPlane::Packet("wg".into())));
    assert!(pins.permits(&second, &PacketPlane::Flow));
}

#[test]
fn packet_plane_stays_pinned_when_route_selection_changes() {
    let mut packet = vec![0_u8; 28];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&28_u16.to_be_bytes());
    packet[9] = 1;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[10, 0, 0, 3]);
    let mut pins = PacketSessionPins::default();
    let wireguard = PacketPlane::Packet("wg".into());
    assert!(pins.permits(&packet, &wireguard));
    assert!(pins.record(&packet, wireguard.clone()));
    assert!(pins.permits(&packet, &wireguard));
    assert!(!pins.permits(&packet, &PacketPlane::Flow));
    assert!(!pins.permits(&packet, &PacketPlane::Packet("other".into())));
    assert!(pins.permits(&packet, &PacketPlane::Packet("wg".into())));
}

#[test]
fn traffic_native_packet_pins_count_roles_global_and_peers_once_and_retire_on_drop() {
    use zero_api::*;
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[{"tag":"a","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":18889},"protocol":{"type":"wireguard","private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],"peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:9","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let engine = zero_engine::Engine::new(config).unwrap();
    let binding = engine.config().endpoint_bindings().remove(0);
    let source = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into(), "peer-2".into()])
        .unwrap();
    source.publish();
    let stat = |scope| engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap();
    let endpoint = TrafficScope::Endpoint {
        endpoint_id: binding.endpoint_id.clone(),
    };
    let peer = |id: &str| TrafficScope::Peer {
        endpoint_id: binding.endpoint_id.clone(),
        peer_id: id.into(),
    };
    let provider = engine.clone();
    let mut pins =
        PacketSessionPins::with_meters(std::sync::Arc::new(move |tag, input, output| {
            provider.packet_route_traffic_meters("endpoint/a", tag, input, output)
        }));
    let packet = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.168.1.119".parse().unwrap(),
        40000,
        443,
        b"packet",
    );
    let plane = PacketPlane::Packet("a".into());
    for _ in 0..2 {
        assert!(pins.record_peers(
            &packet,
            plane.clone(),
            Some("peer-1".into()),
            Some("peer-1".into())
        ));
    }
    for scope in [
        TrafficScope::Global,
        TrafficScope::Inbound {
            tag: "endpoint/a".into(),
        },
        TrafficScope::Outbound { tag: "a".into() },
        endpoint.clone(),
        peer("peer-1"),
    ] {
        assert_eq!(stat(scope).activity.active_packet_routes, Some(1));
    }
    assert_eq!(stat(peer("peer-1")).activity.active_datagram_flows, None);
    assert_eq!(
        engine.stats_snapshot().bytes_up,
        0,
        "Packet must not manufacture billed Flow bytes"
    );
    assert!(pins.permits_peer(
        &packet,
        &PacketPlane::TranslatedPacket("a".into()),
        Some("peer-2".into())
    ));
    assert!(pins.record_peers(
        &packet,
        PacketPlane::TranslatedPacket("a".into()),
        Some("peer-2".into()),
        None
    ));
    assert_eq!(
        stat(TrafficScope::Global).activity.active_packet_routes,
        Some(2)
    );
    assert_eq!(
        stat(endpoint.clone()).activity.active_packet_routes,
        Some(2)
    );
    assert_eq!(stat(peer("peer-1")).activity.active_packet_routes, Some(1));
    assert_eq!(stat(peer("peer-2")).activity.active_packet_routes, Some(1));
    // Switching the target peer keeps global/resource leases stable.
    assert!(pins.record_peers(&packet, plane, Some("peer-1".into()), Some("peer-2".into())));
    assert_eq!(
        stat(TrafficScope::Global).activity.active_packet_routes,
        Some(2)
    );
    assert_eq!(stat(peer("peer-1")).activity.active_packet_routes, Some(1));
    assert_eq!(stat(peer("peer-2")).activity.active_packet_routes, Some(2));
    let before = stat(endpoint.clone());
    let rejected = engine
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: before.core_instance_id,
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: endpoint.clone(),
                expected_stats_epoch: before.stats_epoch,
                expected_generation: before.generation,
            }],
        })
        .unwrap_err();
    assert_eq!(rejected.code, ApiErrorCode::Unsupported);
    assert_eq!(
        stat(endpoint.clone()).activity.active_packet_routes,
        Some(2)
    );
    // Pins expose activity, not cumulative bytes. Global has real Flow counters
    // and can reset its observation period without disturbing those same pins.
    let before = stat(TrafficScope::Global);
    let reset = engine
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: before.core_instance_id,
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: TrafficScope::Global,
                expected_stats_epoch: before.stats_epoch,
                expected_generation: before.generation,
            }],
        })
        .unwrap();
    assert_eq!(reset.snapshots[0].activity.active_packet_routes, Some(2));
    drop(pins);
    assert_eq!(
        stat(TrafficScope::Global).activity.active_packet_routes,
        Some(0)
    );
    assert_eq!(stat(peer("peer-1")).activity.active_packet_routes, Some(0));
    assert_eq!(stat(endpoint).activity.active_packet_routes, Some(0));
}
