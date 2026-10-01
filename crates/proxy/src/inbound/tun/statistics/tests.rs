use super::*;
use zero_api::{StatsResetCommand, StatsResetTarget, TrafficGetQuery};
use zero_core::{Address, Network, ProtocolType, Session};

#[test]
fn traffic_tun_period_reset_isolated_from_global_packets_and_live_business_usage() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap();
    let engine = Engine::new(config).unwrap();
    let source = TunTraffic::prepare(&engine, "host-tun");
    let role = TrafficScope::Inbound {
        tag: "host-tun".into(),
    };
    let stat = |scope| engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap();
    let mut flow = Session::new(
        0,
        Address::Ipv4([198, 51, 100, 10]),
        80,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut flow, "host-tun").unwrap();
    engine.record_session_upload(flow.id, 7);
    source.received(60);
    source.sent(40);
    source.dropped();
    source.error();
    let before = stat(role.clone());
    assert_eq!(before.planes[1].counters.rx_bytes, Some(60));
    assert_eq!(before.planes[1].counters.rx_packets, Some(1));
    assert_eq!(before.planes[1].counters.errors, Some(1));
    assert_eq!(before.planes[2].counters.rx_bytes, None);
    assert_eq!(before.activity.active_stream_flows, Some(1));
    let cleared = engine
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: before.core_instance_id,
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: role.clone(),
                expected_stats_epoch: before.stats_epoch,
                expected_generation: None,
            }],
        })
        .unwrap();
    assert_eq!(cleared.snapshots[0].planes[1].counters.rx_bytes, Some(0));
    assert_eq!(cleared.snapshots[0].activity.active_stream_flows, Some(1));
    assert_eq!(engine.stats_snapshot().bytes_up, 7);
    assert_eq!(engine.active_sessions()[0].bytes_up, 7);
    assert_eq!(
        stat(TrafficScope::Global).planes[1].counters.rx_bytes,
        Some(60)
    );
    source.received(25);
    assert_eq!(stat(role.clone()).planes[1].counters.rx_bytes, Some(25));
    assert_eq!(
        stat(TrafficScope::Global).planes[1].counters.rx_bytes,
        Some(85)
    );
    drop(source);
    assert_eq!(
        engine
            .traffic_snapshot(&TrafficGetQuery { scope: role })
            .unwrap_err()
            .code,
        zero_api::ApiErrorCode::NotFound
    );
    engine
        .finish_session(flow.id, zero_engine::SessionOutcome::DirectRelayed)
        .unwrap();
    assert_eq!(engine.stats_snapshot().bytes_up, 7);
}
