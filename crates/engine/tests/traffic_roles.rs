use crate::traffic_recovery::engine;
use zero_api::*;
use zero_core::{Address, Network, ProtocolType, Session};
#[test]
fn declared_and_dynamic_tun_roles_share_live_meters_and_retire_only_after_teardown() {
    let engine = engine();
    let one = engine.register_inbound_traffic("dynamic-tun");
    let two = engine.register_inbound_traffic("dynamic-tun");
    let mut session = Session::new(
        0,
        Address::Ipv4([198, 51, 100, 10]),
        80,
        Network::Udp,
        ProtocolType::new("tun"),
    );
    engine.prepare_session(&mut session, "dynamic-tun").unwrap();
    engine.record_session_inbound_rx(session.id, 17);
    let query = TrafficGetQuery {
        scope: TrafficScope::Inbound {
            tag: "dynamic-tun".into(),
        },
    };
    let before = engine.traffic_snapshot(&query).unwrap();
    assert_eq!(before.planes[0].counters.rx_bytes, Some(17));
    assert_eq!(before.activity.active_datagram_flows, Some(1));
    engine
        .reload_runtime_config((*engine.config()).clone())
        .unwrap();
    drop(one);
    assert_eq!(
        engine.traffic_snapshot(&query).unwrap().stats_epoch,
        before.stats_epoch
    );
    engine
        .finish_session(session.id, zero_engine::SessionOutcome::DirectRelayed)
        .unwrap();
    drop(two);
    assert_eq!(
        engine.traffic_snapshot(&query).unwrap_err().code,
        ApiErrorCode::NotFound
    );
    assert_eq!(engine.stats_snapshot().bytes_up, 17);
    let three = engine.register_inbound_traffic("dynamic-tun");
    assert_ne!(
        engine.traffic_snapshot(&query).unwrap().stats_epoch,
        before.stats_epoch
    );
    drop(three);
    let mut config = (*engine.config()).clone();
    config.runtime.tun = Some(
        serde_json::from_value(
            serde_json::json!({"addr":"10.66.0.1","tag":"declared-tun","auto_route":false}),
        )
        .unwrap(),
    );
    engine.reload_runtime_config(config).unwrap();
    let declared = TrafficGetQuery {
        scope: TrafficScope::Inbound {
            tag: "declared-tun".into(),
        },
    };
    let epoch = engine.traffic_snapshot(&declared).unwrap().stats_epoch;
    let active = engine.register_inbound_traffic("declared-tun");
    drop(active);
    assert_eq!(
        engine.traffic_snapshot(&declared).unwrap().stats_epoch,
        epoch
    );
}

#[test]
fn peer_flow_claims_activity_and_failures_are_deduplicated_across_roles() {
    let engine = engine();
    let mut config = (*engine.config()).clone();
    config.endpoints[0].listen = Some(
        serde_json::from_value(serde_json::json!({"address":"127.0.0.1","port":18889})).unwrap(),
    );
    config.endpoints[0].directions.inbound = true;
    config.outbounds.retain(|outbound| outbound.tag != "a");
    engine.reload_runtime_config(config).unwrap();
    let binding = engine.config().endpoint_bindings().remove(0);
    let source = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .unwrap();
    source.publish();
    let peer_scope = TrafficScope::Peer {
        endpoint_id: binding.endpoint_id.clone(),
        peer_id: "peer-1".into(),
    };
    let endpoint_scope = TrafficScope::Endpoint {
        endpoint_id: binding.endpoint_id,
    };
    let stat = |scope| engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap();
    let initial = stat(peer_scope.clone());
    assert_eq!(initial.planes[0].counters.bytes_up, None);
    assert_eq!(initial.activity.active_stream_flows, None);
    for network in [Network::Tcp, Network::Udp] {
        let mut session = Session::new(
            0,
            Address::Ipv4([198, 51, 100, 10]),
            80,
            network,
            ProtocolType::new("test"),
        );
        session.inbound_peer_identity = Some("peer-1".into());
        engine.prepare_session(&mut session, "endpoint/a").unwrap();
        session.outbound_tag = Some("a".into());
        engine.set_session_outbound(&session);
        engine.bind_session_peer(session.id, "a", true, "peer-1");
        // Repeated binding must retain the same claims and one live flow lease.
        engine.bind_session_peer(session.id, "a", true, "peer-1");
        engine.record_session_upload(session.id, 13);
        engine.record_session_download(session.id, 17);
        let peer = stat(peer_scope.clone());
        assert_eq!(peer.planes[0].counters.bytes_up, Some(13));
        assert_eq!(peer.planes[0].counters.bytes_down, Some(17));
        assert_eq!(
            peer.planes[0].source_roles,
            vec![TrafficRole::Inbound, TrafficRole::Outbound]
        );
        assert_eq!(
            peer.activity.active_stream_flows,
            Some((network == Network::Tcp) as u64)
        );
        assert_eq!(
            peer.activity.active_datagram_flows,
            Some((network == Network::Udp) as u64)
        );
        let legacy = engine.stats_snapshot().bytes_up;
        let reset = engine
            .reset_traffic(&StatsResetCommand {
                expected_core_instance_id: peer.core_instance_id.clone(),
                operation_id: None,
                targets: vec![StatsResetTarget {
                    scope: peer_scope.clone(),
                    expected_stats_epoch: peer.stats_epoch,
                    expected_generation: peer.generation,
                }],
            })
            .unwrap();
        assert_eq!(reset.snapshots[0].activity, peer.activity);
        assert_eq!(reset.snapshots[0].planes[0].counters.bytes_up, Some(0));
        assert_eq!(engine.stats_snapshot().bytes_up, legacy);
        assert_eq!(
            stat(endpoint_scope.clone()).planes[0].counters.bytes_up,
            Some(legacy)
        );
        engine.record_session_upload(session.id, 3);
        assert_eq!(
            stat(peer_scope.clone()).planes[0].counters.bytes_up,
            Some(3)
        );
        engine
            .finish_session(session.id, zero_engine::SessionOutcome::Failed)
            .unwrap();
        assert!(engine
            .finish_session(session.id, zero_engine::SessionOutcome::Failed)
            .is_none());
        let peer = stat(peer_scope.clone());
        assert_eq!(peer.planes[0].counters.errors, Some(1));
        assert_eq!(peer.activity.active_stream_flows, Some(0));
        assert_eq!(peer.activity.active_datagram_flows, Some(0));
        engine
            .reset_traffic(&StatsResetCommand {
                expected_core_instance_id: peer.core_instance_id,
                operation_id: None,
                targets: vec![StatsResetTarget {
                    scope: peer_scope.clone(),
                    expected_stats_epoch: peer.stats_epoch,
                    expected_generation: peer.generation,
                }],
            })
            .unwrap();
    }
    assert_eq!(
        stat(TrafficScope::Global).planes[0].counters.errors,
        Some(2)
    );
    assert_eq!(stat(endpoint_scope).planes[0].counters.errors, Some(2));
    let mut cancelled = Session::new(
        0,
        Address::Ipv4([198, 51, 100, 10]),
        80,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut cancelled, "a").unwrap();
    engine
        .finish_session(cancelled.id, zero_engine::SessionOutcome::Cancelled)
        .unwrap();
    assert_eq!(
        stat(TrafficScope::Global).planes[0].counters.errors,
        Some(2)
    );
}
