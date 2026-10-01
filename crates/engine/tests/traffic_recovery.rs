use zero_api::*;
use zero_config::RuntimeConfig;
use zero_engine::{Engine, EngineHandle};
pub(crate) fn engine() -> Engine {
    Engine::new(RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[{"tag":"a","protocol":{"type":"wireguard","private_key":"01".repeat(32),"addresses":["10.0.0.1/32"],"peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:9","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap()
}
#[test]
fn retired_device_handles_cannot_resurrect_scopes_or_old_periods() {
    let engine = engine();
    let binding = engine.config().endpoint_bindings().remove(0);
    let prepared = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .unwrap();
    prepared
        .endpoint()
        .enable(TrafficPlane::Inner, &[TrafficMetric::RxBytes]);
    prepared.publish();
    let scope = TrafficScope::Endpoint {
        endpoint_id: binding.endpoint_id.clone(),
    };
    let before = engine
        .traffic_snapshot(&TrafficGetQuery {
            scope: scope.clone(),
        })
        .unwrap();
    let mut config = (*engine.config()).clone();
    config.endpoints.clear();
    config.inbounds.clear();
    config.outbounds.clear();
    engine.reload_runtime_config(config).unwrap();
    prepared.endpoint().received(TrafficPlane::Inner, 100);
    prepared.publish();
    assert_eq!(
        engine
            .traffic_snapshot(&TrafficGetQuery {
                scope: scope.clone()
            })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
    assert_eq!(
        engine
            .reset_traffic(&StatsResetCommand {
                expected_core_instance_id: before.core_instance_id.clone(),
                operation_id: None,
                targets: vec![StatsResetTarget {
                    scope,
                    expected_stats_epoch: before.stats_epoch.clone(),
                    expected_generation: before.generation
                }]
            })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
    assert!(engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .is_none());
    engine
        .reload_runtime_config((*self::engine().config()).clone())
        .unwrap();
    let current = engine
        .traffic_snapshot(&TrafficGetQuery {
            scope: before.scope,
        })
        .unwrap();
    assert_ne!(current.stats_epoch, before.stats_epoch);
    assert_eq!(current.planes[1].counters.rx_bytes, None);
    prepared.publish();
    assert_eq!(
        engine
            .traffic_snapshot(&TrafficGetQuery {
                scope: current.scope
            })
            .unwrap()
            .stats_epoch,
        current.stats_epoch
    );
}
#[test]
fn slow_sample_consumers_have_bounded_queues_and_can_recover_from_replay_gaps() {
    let engine = engine();
    let handle = EngineHandle::new(engine.clone());
    let filter = EventFilter {
        event_types: vec![event_type::STATS_SCOPES_SAMPLED.into()],
        ..Default::default()
    };
    let stream = handle.subscribe(filter.clone()).unwrap();
    let uninteresting = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::STATS_RESET.into()],
            ..Default::default()
        })
        .unwrap();
    // Large observation events exercise the byte budget before the 1024-slot
    // subscriber bound. Real sampling pages are limited to 64 scopes.
    let payload = serde_json::json!({"test_large_observation":"x".repeat(100_000)});
    for index in 0..120 {
        handle.emit(ApiEvent::new(
            format!("sample-{index}"),
            event_type::STATS_SCOPES_SAMPLED,
            0,
            payload.clone(),
        ));
    }
    let mut retained = Vec::new();
    while let Some(event) = stream.try_recv() {
        retained.push(event.sequence.unwrap());
    }
    assert!(retained.len() <= 10);
    assert!(uninteresting.try_recv().is_none());
    let replay = handle.since(0, 256, filter).unwrap();
    assert!(replay.has_gap);
    assert!(replay.events.len() <= 83);
    engine.push_traffic_stats_sampled();
    let fresh = stream.try_recv().unwrap();
    assert!(fresh.sequence.unwrap() > retained.last().unwrap() + 1);
    let page = engine
        .traffic_snapshots(&TrafficListQuery::default())
        .unwrap();
    assert_eq!(page.core_instance_id, fresh.core_instance_id.unwrap());
    assert_eq!(page.scopes[0].planes[0].counters.bytes_up, Some(0));
}
#[test]
fn large_scope_sampling_rotates_bounded_pages_and_recovers_after_configuration_change() {
    let engine = engine();
    let mut config = (*engine.config()).clone();
    let prototype = config.outbounds[0].clone();
    for index in 0..300 {
        let mut outbound = prototype.clone();
        outbound.tag = format!("wg-{index}");
        config.outbounds.push(outbound);
    }
    engine.reload_runtime_config(config).unwrap();
    let handle = EngineHandle::new(engine.clone());
    let stream = handle
        .subscribe(EventFilter {
            event_types: vec![event_type::STATS_SCOPES_SAMPLED.into()],
            ..Default::default()
        })
        .unwrap();
    let mut observed = std::collections::BTreeSet::new();
    for _ in 0..6 {
        engine.push_traffic_stats_sampled();
        let event = stream.try_recv().unwrap();
        let page: TrafficListSnapshot = serde_json::from_value(event.payload).unwrap();
        assert!(page.scopes.len() <= 64);
        observed.extend(page.scopes.into_iter().map(|s| s.scope));
    }
    let first = engine
        .traffic_snapshots(&TrafficListQuery::default())
        .unwrap();
    assert_eq!(observed.len(), first.total);
    engine
        .reload_runtime_config((*self::engine().config()).clone())
        .unwrap();
    assert_eq!(
        engine
            .traffic_snapshots(&TrafficListQuery {
                offset: 64,
                expected_core_instance_id: Some(first.core_instance_id),
                expected_config_revision: Some(first.config_revision),
                expected_registry_revision: Some(first.registry_revision),
                ..Default::default()
            })
            .unwrap_err()
            .code,
        ApiErrorCode::Conflict
    );
}
