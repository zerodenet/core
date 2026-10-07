use super::{PacketPlane, PacketSessionPins};

#[test]
fn admitted_packet_paths_prepare_observers_once_and_release_them_on_expiry() {
    use std::sync::Arc;
    #[derive(Debug)]
    struct Observer;
    impl zero_traits::IoObserver for Observer {
        fn received(&self, _: usize) {}
        fn sent(&self, _: usize) {}
        fn error(&self) {}
        fn dropped(&self) {}
    }
    let packet = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"packet",
    );
    let plane = PacketPlane::Packet("wg".into());
    let peer: Option<Arc<str>> = Some("peer-1".into());
    let observer: Arc<dyn zero_traits::IoObserver> = Arc::new(Observer);
    let weak = Arc::downgrade(&observer);
    let mut pins = PacketSessionPins::default();
    let prepared = pins.inner_io(&packet, &plane, peer.clone(), || Some(observer.clone()));
    assert!(pins.record_observed_peers(&packet, plane.clone(), peer.clone(), None, prepared));
    drop(observer);
    let reused = pins.inner_io(&packet, &plane, peer.clone(), || {
        panic!("per-packet preparation")
    });
    assert!(Arc::ptr_eq(
        reused.as_ref().unwrap(),
        &weak.upgrade().unwrap()
    ));
    drop(reused);
    // A measured-unavailable result is cached too. Different ingress peer
    // identities retain their own path and never share an attribution handle.
    let other = Some("peer-2".into());
    assert!(pins.record_observed_peers(&packet, plane.clone(), other.clone(), None, None));
    assert!(pins
        .inner_io(&packet, &plane, other, || panic!(
            "unavailable provider rebuilt"
        ))
        .is_none());
    for pin in pins.entries.values_mut() {
        pin.touched -= super::IDLE_TIMEOUT;
    }
    pins.expire();
    assert!(weak.upgrade().is_none());
    let mut prepared_again = false;
    assert!(pins
        .inner_io(&packet, &plane, peer, || {
            prepared_again = true;
            None
        })
        .is_none());
    assert!(prepared_again);
}

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

#[test]
fn role_revocation_releases_only_the_denied_packet_pins() {
    let first = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"first",
    );
    let second = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.4".parse().unwrap(),
        40000,
        443,
        b"second",
    );
    let mut pins = PacketSessionPins::default();
    assert!(pins.record(&first, PacketPlane::Packet("company".into())));
    assert!(pins.record(&second, PacketPlane::Packet("personal".into())));
    pins.retain_admitted(|plane| plane != &PacketPlane::Packet("company".into()));
    assert_eq!(pins.entries.len(), 1);
    assert!(pins.permits(&first, &PacketPlane::Flow));
    assert!(!pins.permits(&second, &PacketPlane::Flow));
}

#[tokio::test]
async fn closing_one_managed_packet_lease_cancels_its_response_queue_and_preserves_another() {
    use tokio::{
        sync::mpsc,
        time::{timeout, Duration},
    };
    let engine = zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "tun".into());
    let (responses, mut received) = mpsc::channel(8);
    let a = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"a",
    );
    let b = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40001,
        443,
        b"b",
    );
    let plane = PacketPlane::Packet("wg".into());
    let first = pins
        .replies_for(&a, &plane, None, responses.clone())
        .unwrap();
    pins.record_observed_peers(&a, plane.clone(), None, None, None);
    let second = pins.replies_for(&b, &plane, None, responses).unwrap();
    pins.record_observed_peers(&b, plane.clone(), None, None, None);
    first.send(a.clone()).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap(),
        a
    );
    let list = engine.packet_routes_snapshot(&zero_api::PacketRouteListQuery::default());
    assert_eq!(list.total, 2);
    let (_, close) = engine
        .begin_close_packet_route(&zero_api::PacketRouteCloseCommand {
            route_id: list.routes[0].route_id.clone(),
            expected_core_instance_id: engine.core_instance_id().into(),
            expected_config_revision: None,
        })
        .unwrap();
    timeout(Duration::from_secs(1), pins.management_changed())
        .await
        .unwrap();
    pins.expire();
    timeout(Duration::from_secs(1), close.wait_released())
        .await
        .unwrap();
    assert!(first.is_closed());
    assert!(!second.is_closed());
    assert!(!pins.permits(&a, &plane));
    assert!(pins.permits(&b, &plane));
    second.send(b.clone()).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap(),
        b
    );
    assert_eq!(
        engine
            .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
            .total,
        1
    );
    drop(pins);
    timeout(Duration::from_secs(1), second.closed())
        .await
        .unwrap();
}
#[tokio::test]
async fn rejected_provisional_packet_path_releases_its_fact_and_permits_fallback() {
    let engine = zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "tun".into());
    let (responses, _) = tokio::sync::mpsc::channel(1);
    let packet = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"a",
    );
    pins.replies_for(&packet, &PacketPlane::Packet("wg".into()), None, responses)
        .unwrap();
    let control = pins
        .entries
        .values()
        .next()
        .unwrap()
        .control
        .as_ref()
        .unwrap()
        .clone();
    pins.reject_unaccepted(&packet, None);
    tokio::time::timeout(std::time::Duration::from_secs(1), control.wait_released())
        .await
        .unwrap();
    assert_eq!(
        engine
            .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
            .total,
        0
    );
    assert!(pins.permits(&packet, &PacketPlane::Flow));
}

#[tokio::test]
async fn close_wakes_idle_owner_even_when_the_response_consumer_is_blocked() {
    let engine = zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "tun".into());
    let packet = zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"payload",
    );
    let plane = PacketPlane::Packet("wg".into());
    let (destination, _consumer) = tokio::sync::mpsc::channel(1);
    destination.send(vec![0]).await.unwrap();
    let replies = pins
        .replies_for(&packet, &plane, None, destination)
        .unwrap();
    pins.record(&packet, plane);
    replies.send(packet.clone()).await.unwrap();
    tokio::task::yield_now().await;
    let route = engine
        .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
        .routes
        .remove(0);
    let (_, control) = engine
        .begin_close_packet_route(&zero_api::PacketRouteCloseCommand {
            route_id: route.route_id,
            expected_core_instance_id: engine.core_instance_id().into(),
            expected_config_revision: None,
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), pins.management_changed())
        .await
        .unwrap();
    pins.expire();
    tokio::time::timeout(std::time::Duration::from_secs(1), control.wait_released())
        .await
        .unwrap();
    assert!(replies.is_closed());
    assert_eq!(
        engine
            .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
            .total,
        0
    );
}
