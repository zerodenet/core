use std::sync::{Arc, Barrier};
use zero_api::*;
use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{Engine, SessionOutcome};
fn engine() -> Engine {
    Engine::new(RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":18888},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"out","protocol":{"type":"direct"}}],
        "endpoints":[{"tag":"a","listen":{"address":"127.0.0.1","port":18889},"protocol":{"type":"wireguard","private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],"peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:9","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap()
}
fn stat(engine: &Engine, scope: TrafficScope) -> TrafficSnapshot {
    engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap()
}
fn request(snapshot: &TrafficSnapshot) -> StatsResetCommand {
    StatsResetCommand {
        expected_core_instance_id: snapshot.core_instance_id.clone(),
        operation_id: Some("test".into()),
        targets: vec![StatsResetTarget {
            scope: snapshot.scope.clone(),
            expected_stats_epoch: snapshot.stats_epoch.clone(),
            expected_generation: snapshot.generation,
        }],
    }
}
fn flow(engine: &Engine, network: Network, incoming: &str, outgoing: &str) -> Session {
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        80,
        network,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut session, incoming).unwrap();
    session.outbound_tag = Some(outgoing.into());
    engine.set_session_outbound(&session);
    session
}
#[test]
fn live_tcp_udp_roles_reset_without_changing_business_or_connection_totals() {
    let engine = engine();
    for (iteration, network) in [Network::Tcp, Network::Udp].into_iter().enumerate() {
        let session = flow(&engine, network, "in", "out");
        engine.record_session_inbound_rx(session.id, 17);
        engine.record_session_outbound_tx(session.id, 17);
        engine.record_session_outbound_rx(session.id, 23);
        engine.record_session_inbound_tx(session.id, 23);
        let global = stat(&engine, TrafficScope::Global);
        assert_eq!(global.planes[0].counters.bytes_up, Some(17));
        let outbound = stat(&engine, TrafficScope::Outbound { tag: "out".into() });
        assert_eq!(outbound.planes[0].counters.tx_bytes, Some(17));
        assert_eq!(outbound.planes[0].counters.rx_bytes, Some(23));
        assert_eq!(outbound.planes[0].counters.rx_packets, None);
        assert_eq!(outbound.planes[2].counters.rx_bytes, None);
        let before = engine.active_sessions()[0].clone();
        let reset = engine.reset_traffic(&request(&outbound)).unwrap();
        assert_eq!(reset.snapshots[0].planes[0].counters.tx_bytes, Some(0));
        assert_eq!(
            reset.snapshots[0].activity.active_stream_flows,
            Some((network == Network::Tcp) as u64)
        );
        assert_eq!(engine.active_sessions()[0].bytes_up, before.bytes_up);
        assert_eq!(
            stat(&engine, TrafficScope::Inbound { tag: "in".into() }).planes[0]
                .counters
                .rx_bytes,
            Some(17)
        );
        engine.record_session_upload(session.id, 5);
        assert_eq!(
            stat(&engine, TrafficScope::Outbound { tag: "out".into() }).planes[0]
                .counters
                .tx_bytes,
            Some(5)
        );
        engine
            .finish_session(session.id, SessionOutcome::DirectRelayed)
            .unwrap();
        assert_eq!(
            stat(&engine, TrafficScope::Outbound { tag: "out".into() }).planes[0]
                .counters
                .tx_bytes,
            Some(5)
        );
        assert_eq!(
            engine.stats_snapshot().bytes_up,
            22 * (iteration as u64 + 1)
        );
        engine
            .reset_traffic(&request(&stat(&engine, TrafficScope::Global)))
            .unwrap();
        // Each observation period isolates its baselines; legacy totals remain.
        let inbound = stat(&engine, TrafficScope::Inbound { tag: "in".into() });
        engine.reset_traffic(&request(&inbound)).unwrap();
        let outbound = stat(&engine, TrafficScope::Outbound { tag: "out".into() });
        engine.reset_traffic(&request(&outbound)).unwrap();
    }
}
#[test]
fn concurrent_resets_use_epoch_cas_and_atomic_batch_validation() {
    let engine = engine();
    let snapshot = stat(&engine, TrafficScope::Global);
    let command = request(&snapshot);
    let barrier = Arc::new(Barrier::new(3));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let e = engine.clone();
            let b = barrier.clone();
            let c = command.clone();
            std::thread::spawn(move || {
                b.wait();
                e.reset_traffic(&c)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter_map(|r| r.as_ref().err())
            .next()
            .unwrap()
            .code,
        ApiErrorCode::Conflict
    );
    let current = stat(&engine, TrafficScope::Global);
    let mut bad = request(&current);
    bad.targets.push(StatsResetTarget {
        scope: TrafficScope::Endpoint {
            endpoint_id: "missing".into(),
        },
        expected_stats_epoch: "x".into(),
        expected_generation: None,
    });
    assert_eq!(
        engine.reset_traffic(&bad).unwrap_err().code,
        ApiErrorCode::NotFound
    );
    assert_eq!(
        stat(&engine, TrafficScope::Global).stats_epoch,
        current.stats_epoch
    );
    bad = request(&current);
    bad.expected_core_instance_id = "old-process".into();
    assert_eq!(
        engine
            .reset_traffic(&bad)
            .unwrap_err()
            .field_path
            .as_deref(),
        Some("params.expected_core_instance_id")
    );
    bad = request(&current);
    bad.targets.push(bad.targets[0].clone());
    assert_eq!(
        engine.reset_traffic(&bad).unwrap_err().code,
        ApiErrorCode::InvalidArgument
    );
}
#[test]
fn shared_endpoint_deduplicates_flow_and_has_independent_inner_outer_periods() {
    let engine = engine();
    let session = flow(&engine, Network::Udp, "endpoint/a", "a");
    engine.record_session_upload(session.id, 100);
    engine.record_session_download(session.id, 50);
    let scope = TrafficScope::Endpoint {
        endpoint_id: "endpoint:a".into(),
    };
    let endpoint = stat(&engine, scope.clone());
    assert_eq!(endpoint.planes[0].counters.bytes_up, Some(100));
    assert_eq!(endpoint.activity.active_datagram_flows, Some(1));
    let binding = engine
        .config()
        .endpoint_bindings()
        .into_iter()
        .find(|b| b.canonical)
        .unwrap();
    let prepared = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .unwrap();
    let meter = prepared.endpoint();
    let peer = prepared.peers().remove(0);
    for m in [&meter, &peer] {
        m.enable(
            TrafficPlane::Inner,
            &[TrafficMetric::RxBytes, TrafficMetric::RxPackets],
        );
    }
    prepared.publish();
    meter.received(TrafficPlane::Inner, 80);
    peer.received(TrafficPlane::Inner, 80);
    let lease = meter.packet_route();
    let snapshot = stat(&engine, scope.clone());
    let reset = engine.reset_traffic(&request(&snapshot)).unwrap();
    assert_eq!(reset.snapshots[0].activity.active_packet_routes, Some(1));
    assert_eq!(reset.snapshots[0].planes[1].counters.rx_bytes, Some(0));
    assert_eq!(
        stat(
            &engine,
            TrafficScope::Peer {
                endpoint_id: "endpoint:a".into(),
                peer_id: "peer-1".into()
            }
        )
        .planes[1]
            .counters
            .rx_bytes,
        Some(80)
    );
    assert_eq!(engine.stats_snapshot().bytes_up, 100);
    assert_eq!(engine.active_sessions()[0].bytes_up, 100);
    drop(lease);
    assert_eq!(stat(&engine, scope).activity.active_packet_routes, Some(0));
}
#[test]
fn paged_identity_recovery_peer_retirement_and_reset_event_are_bounded() {
    let engine = engine();
    let first = engine
        .traffic_snapshots(&TrafficListQuery {
            limit: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.scopes.len(), 1);
    assert!(first.next_offset.is_some());
    let binding = engine
        .config()
        .endpoint_bindings()
        .into_iter()
        .find(|b| b.canonical)
        .unwrap();
    engine
        .prepare_endpoint_traffic(&binding, &["one".into(), "two".into()])
        .unwrap()
        .publish();
    assert_eq!(
        engine
            .traffic_snapshots(&TrafficListQuery {
                expected_registry_revision: Some(first.registry_revision),
                ..Default::default()
            })
            .unwrap_err()
            .code,
        ApiErrorCode::Conflict
    );
    engine
        .prepare_endpoint_traffic(&binding, &["one".into()])
        .unwrap()
        .publish();
    assert_eq!(
        engine
            .traffic_snapshot(&TrafficGetQuery {
                scope: TrafficScope::Peer {
                    endpoint_id: binding.endpoint_id.clone(),
                    peer_id: "two".into()
                }
            })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
    let snapshot = stat(&engine, TrafficScope::Global);
    engine.reset_traffic(&request(&snapshot)).unwrap();
    engine.push_traffic_stats_sampled();
    let events = engine.events_snapshot(&EventFilter::default());
    let reset = events
        .iter()
        .find(|e| e.event_type == event_type::STATS_RESET)
        .unwrap();
    assert_eq!(
        reset.payload["snapshots"][0]["stats_epoch"],
        stat(&engine, TrafficScope::Global).stats_epoch
    );
    assert!(events
        .iter()
        .any(|e| e.event_type == event_type::STATS_SCOPES_SAMPLED));
    let restart = crate::traffic_observation::engine();
    assert_ne!(restart.core_instance_id(), engine.core_instance_id());
    assert_ne!(
        stat(&restart, TrafficScope::Global).stats_epoch,
        stat(&engine, TrafficScope::Global).stats_epoch
    );
}

#[test]
fn reset_during_receive_preserves_monotonic_usage_and_quota_exhaustion() {
    let engine = engine();
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        80,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    let mut auth = zero_core::SessionAuth::new("test");
    auth.principal_key = Some("account:stats".into());
    auth.policy_revision = Some(1);
    auth.quota_remaining_bytes = Some(100);
    session.apply_auth(auth);
    engine.prepare_session(&mut session, "in").unwrap();
    let handle = engine.track_session(session.id);
    engine.record_session_inbound_rx(session.id, 60);
    engine
        .reset_traffic(&request(&stat(&engine, TrafficScope::Global)))
        .unwrap();
    assert!(!handle.is_cancelled());
    engine.record_session_inbound_rx(session.id, 40);
    assert!(handle.is_cancelled());
    assert_eq!(engine.stats_snapshot().bytes_up, 100);
    assert_eq!(
        stat(&engine, TrafficScope::Global).planes[0]
            .counters
            .bytes_up,
        Some(40)
    );
    assert_eq!(engine.active_sessions()[0].bytes_up, 100);
    // Concurrent writes remain on the same monotonic source, across many periods.
    let other = crate::traffic_observation::engine();
    let session = flow(&other, Network::Udp, "in", "out");
    let writer = other.clone();
    let task = std::thread::spawn(move || {
        for _ in 0..10_000 {
            writer.record_session_upload(session.id, 1);
        }
    });
    for _ in 0..20 {
        let before = stat(&other, TrafficScope::Global);
        let result = other.reset_traffic(&request(&before)).unwrap();
        let after = stat(&other, TrafficScope::Global);
        assert_eq!(result.snapshots[0].stats_epoch, after.stats_epoch);
        assert!(
            after.planes[0].counters.bytes_up.unwrap()
                >= result.snapshots[0].planes[0].counters.bytes_up.unwrap()
        );
    }
    task.join().unwrap();
    assert_eq!(other.stats_snapshot().bytes_up, 10_000);
    assert_eq!(other.active_sessions()[0].bytes_up, 10_000);
}
