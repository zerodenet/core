use super::super::{PacketPlane, PacketSessionPins, RouteKey, IDLE_TIMEOUT, MAX_PINNED_ROUTES};
use std::time::{Duration, Instant};

fn packet(port: u16) -> Vec<u8> {
    zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        port,
        443,
        b"packet",
    )
}

fn key(port: u16) -> RouteKey {
    (
        zero_stack::packet::packet_conversation_key(&packet(port)).unwrap(),
        None,
    )
}

fn record(pins: &mut PacketSessionPins, key: &RouteKey, plane: PacketPlane, now: Instant) -> bool {
    pins.record_peers_key_at(Some(key.0), plane, key.1.clone(), None, now)
}

#[test]
fn packet_pin_hits_and_renewals_do_not_scan_unexpired_entries() {
    let start = Instant::now();
    let mut pins = PacketSessionPins::default();
    for port in 1..=256 {
        assert!(record(&mut pins, &key(port), PacketPlane::Flow, start));
    }
    let active = key(1);
    let renewed = start + Duration::from_secs(300);
    for _ in 0..1_024 {
        assert!(pins.permits_key_at(&active, &PacketPlane::Flow, renewed));
        assert!(record(&mut pins, &active, PacketPlane::Flow, renewed));
    }
    assert_eq!(pins.expiry_scans, 0);

    // The conservative deadline can trigger one sweep after a renewal. It
    // must neither expire the renewed pin nor repeat that sweep on every hit.
    let original_expiry = start + IDLE_TIMEOUT;
    for _ in 0..1_024 {
        assert!(pins.permits_key_at(&active, &PacketPlane::Flow, original_expiry));
    }
    assert_eq!(pins.expiry_scans, 1);
    assert_eq!(pins.entries.len(), 1);
    let different_plane = PacketPlane::Packet("wg".into());
    let expiry = renewed + IDLE_TIMEOUT;
    assert!(!pins.permits_key_at(&active, &different_plane, expiry - Duration::from_nanos(1)));
    assert!(pins.permits_key_at(&active, &different_plane, expiry));
    assert_eq!(pins.expiry_scans, 2);
    assert!(pins.entries.is_empty());
}

#[test]
fn full_packet_pin_table_reclaims_expired_slots_without_scanning_each_miss() {
    let start = Instant::now();
    let mut pins = PacketSessionPins::default();
    assert!(record(&mut pins, &key(1), PacketPlane::Flow, start));
    for port in 2..=MAX_PINNED_ROUTES as u16 {
        assert!(record(
            &mut pins,
            &key(port),
            PacketPlane::Flow,
            start + Duration::from_secs(1)
        ));
    }
    let missing = key(MAX_PINNED_ROUTES as u16 + 1);
    let expiry = start + IDLE_TIMEOUT;
    for _ in 0..1_024 {
        assert!(!pins.permits_key_at(
            &missing,
            &PacketPlane::Flow,
            expiry - Duration::from_nanos(1)
        ));
    }
    assert!(!record(
        &mut pins,
        &missing,
        PacketPlane::Flow,
        expiry - Duration::from_nanos(1)
    ));
    assert_eq!(pins.entries.len(), MAX_PINNED_ROUTES);
    assert_eq!(pins.expiry_scans, 0);

    // Admission must recover immediately at the exact deadline, without
    // waiting for the owner's periodic expire() call.
    assert!(pins.permits_key_at(&missing, &PacketPlane::Flow, expiry));
    assert!(record(&mut pins, &missing, PacketPlane::Flow, expiry));
    assert_eq!(pins.entries.len(), MAX_PINNED_ROUTES);
    for _ in 0..1_024 {
        assert!(!pins.permits_key_at(&key(1), &PacketPlane::Flow, expiry));
    }
    assert_eq!(pins.expiry_scans, 1);
}

#[test]
fn packet_pin_expiry_preserves_independent_peer_deadlines() {
    let start = Instant::now();
    let mut first = key(1);
    first.1 = Some("first-peer".into());
    let mut second = key(1);
    second.1 = Some("second-peer".into());
    let plane = PacketPlane::Packet("wg".into());
    let mut pins = PacketSessionPins::default();
    assert!(record(&mut pins, &first, plane.clone(), start));
    assert!(record(
        &mut pins,
        &second,
        plane.clone(),
        start + Duration::from_secs(100)
    ));
    let expiry = start + IDLE_TIMEOUT;
    assert!(pins.permits_key_at(&first, &PacketPlane::Flow, expiry));
    assert!(!pins.permits_key_at(&second, &PacketPlane::Flow, expiry));
    assert!(pins.permits_key_at(&second, &plane, expiry));
    assert!(record(&mut pins, &first, PacketPlane::Flow, expiry));
    assert_eq!(pins.entries.len(), 2);
}

#[test]
fn explicit_packet_pin_maintenance_recomputes_renewed_deadlines() {
    let start = Instant::now();
    let mut pins = PacketSessionPins::default();
    let active = key(1);
    assert!(record(&mut pins, &active, PacketPlane::Flow, start));
    let renewed = start + Duration::from_secs(100);
    assert!(record(&mut pins, &active, PacketPlane::Flow, renewed));
    pins.expire_at(renewed);
    assert_eq!(pins.next_expiry, Some(renewed + IDLE_TIMEOUT));
    assert!(pins.permits_key_at(&active, &PacketPlane::Flow, start + IDLE_TIMEOUT));
    assert_eq!(pins.expiry_scans, 1);
    pins.entries.get_mut(&active).unwrap().touched -= IDLE_TIMEOUT * 2;
    pins.expire();
    assert!(pins.entries.is_empty());
    assert_eq!(pins.next_expiry, None);
}

#[tokio::test]
async fn closed_packet_pin_stays_denied_until_exact_idle_expiry() {
    let engine = zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "tun".into());
    let (responses, _received) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let plane = PacketPlane::Packet("wg".into());
    let replies = pins
        .replies_for(&packet(1), &plane, None, responses.into())
        .unwrap();
    assert!(pins.record(&packet(1), plane.clone()));
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
    let route_key = key(1);
    let expiry = pins.entries[&route_key].touched + IDLE_TIMEOUT;
    pins.expire();
    assert_eq!(pins.entries.len(), 1);
    for denied_plane in [&plane, &PacketPlane::Flow] {
        assert!(!pins.permits_key_at(&route_key, denied_plane, expiry - Duration::from_nanos(1)));
    }
    assert!(pins.permits_key_at(&route_key, &PacketPlane::Flow, expiry));
    assert!(pins.entries.is_empty());
    tokio::time::timeout(Duration::from_secs(1), control.wait_released())
        .await
        .unwrap();
    assert!(replies.is_closed());
}
