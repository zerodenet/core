//! Provider periods and local discards reuse the existing statistics registry.
use zero_api::*;
use zero_engine::{Engine, HostInterfaceSample};
fn engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap()
}
fn stat(engine: &Engine, scope: TrafficScope) -> TrafficSnapshot {
    engine.traffic_snapshot(&TrafficGetQuery { scope }).unwrap()
}
fn host() -> TrafficScope {
    TrafficScope::HostInterface {
        name: "test0".into(),
    }
}
fn sample(index: u32, n: u64) -> HostInterfaceSample {
    HostInterfaceSample {
        name: "test0".into(),
        index,
        accounting_basis: "test_host_provider",
        counters: [
            None,
            None,
            Some(n),
            Some(n * 2),
            Some(n),
            Some(n),
            None,
            None,
            Some(n),
            None,
            Some(n),
            Some(n),
        ],
        sampled_at_unix_ms: n + 1000,
    }
}
fn reset(snapshot: &TrafficSnapshot) -> StatsResetCommand {
    StatsResetCommand {
        expected_core_instance_id: snapshot.core_instance_id.clone(),
        operation_id: None,
        targets: vec![StatsResetTarget {
            scope: snapshot.scope.clone(),
            expected_stats_epoch: snapshot.stats_epoch.clone(),
            expected_generation: snapshot.generation,
        }],
    }
}
#[test]
fn observed_drop_reasons_reset_independently_of_business_and_other_scopes() {
    let engine = engine();
    let scope = TrafficScope::Outbound {
        tag: "direct".into(),
    };
    let meter = engine.traffic_meter(&scope).unwrap();
    let initial = stat(&engine, scope.clone());
    assert!(initial.planes[1].drop_reasons.is_empty());
    assert_eq!(initial.planes[1].counters.dropped_packets, None);
    meter.dropped_reason(TrafficPlane::Inner, TrafficDropReason::QueueFull);
    let measured = stat(&engine, scope.clone());
    assert_eq!(measured.planes[1].counters.dropped_packets, None); // Partial role coverage.
    assert_eq!(
        measured.planes[1].drop_reasons,
        vec![TrafficDropCounter {
            reason: TrafficDropReason::QueueFull,
            packets: 1
        }]
    );
    let cleared = engine.reset_traffic(&reset(&measured)).unwrap();
    assert_eq!(cleared.snapshots[0].planes[1].drop_reasons[0].packets, 0);
    meter.dropped_reason(TrafficPlane::Inner, TrafficDropReason::QueueFull);
    assert_eq!(stat(&engine, scope).planes[1].drop_reasons[0].packets, 1);
    assert_eq!(
        stat(&engine, TrafficScope::Global).planes[1]
            .counters
            .dropped_packets,
        None
    );
    assert_eq!(engine.stats_snapshot().bytes_up, 0);
}
#[test]
fn host_stats_have_separate_scope_plane_timestamp_and_non_cascading_reset() {
    let engine = engine();
    engine.observe_host_interfaces(Some(&[sample(7, 100)]));
    engine.observe_host_interfaces(Some(&[sample(7, 110)]));
    let host_before = stat(&engine, host());
    assert_eq!(host_before.planes.len(), 1);
    assert_eq!(host_before.planes[0].plane, TrafficPlane::Host);
    let counters = &host_before.planes[0].counters;
    assert_eq!(counters.rx_bytes, Some(10));
    assert_eq!(counters.tx_bytes, Some(20));
    assert_eq!(counters.rx_dropped_packets, Some(10));
    assert_eq!(counters.tx_dropped_packets, None);
    assert_eq!(counters.dropped_packets, None);
    assert_eq!(host_before.source_sampled_at_unix_ms, Some(1110));
    assert_eq!(host_before.activity, TrafficActivity::default());
    assert!(host_before.planes[0].source_roles.is_empty());
    let global = stat(&engine, TrafficScope::Global);
    let cleared = engine.reset_traffic(&reset(&host_before)).unwrap();
    assert_eq!(cleared.snapshots[0].generation, host_before.generation);
    assert_ne!(cleared.snapshots[0].stats_epoch, host_before.stats_epoch);
    assert_eq!(
        cleared.snapshots[0].planes[0].counters.rx_dropped_packets,
        Some(0)
    );
    assert_eq!(
        stat(&engine, TrafficScope::Global).stats_epoch,
        global.stats_epoch
    );
    assert_eq!(
        engine.reset_traffic(&reset(&host_before)).unwrap_err().code,
        ApiErrorCode::Conflict
    );
    engine.observe_host_interfaces(Some(&[sample(7, 113)]));
    assert_eq!(
        stat(&engine, host()).planes[0].counters.rx_dropped_packets,
        Some(3)
    );
    let page = engine
        .traffic_snapshots(&TrafficListQuery::default())
        .unwrap();
    assert!(!page
        .scopes
        .iter()
        .any(|s| matches!(s.scope, TrafficScope::HostInterface { .. })));
    let page = engine
        .traffic_snapshots(&TrafficListQuery {
            include_host_interfaces: true,
            ..Default::default()
        })
        .unwrap();
    assert!(page.scopes.iter().any(|s| s.scope == host()));
}
#[test]
fn failed_provider_is_unavailable_recovery_keeps_epoch_counter_reset_replaces_generation() {
    let engine = engine();
    engine.observe_host_interfaces(Some(&[sample(7, 100)]));
    let initial = stat(&engine, host());
    engine.observe_host_interfaces(None);
    let unavailable = stat(&engine, host());
    assert_eq!(unavailable.planes[0].counters.rx_bytes, None);
    assert_eq!(
        unavailable.source_sampled_at_unix_ms,
        initial.source_sampled_at_unix_ms
    );
    assert_eq!(
        engine.reset_traffic(&reset(&unavailable)).unwrap_err().code,
        ApiErrorCode::Unsupported
    );
    engine.observe_host_interfaces(Some(&[sample(7, 105)]));
    let recovered = stat(&engine, host());
    assert_eq!(recovered.stats_epoch, initial.stats_epoch);
    assert_eq!(recovered.planes[0].counters.rx_bytes, Some(5));
    engine.observe_host_interfaces(Some(&[sample(7, 2)]));
    let replaced = stat(&engine, host());
    assert_ne!(replaced.generation, initial.generation);
    assert_ne!(replaced.stats_epoch, initial.stats_epoch);
    assert_eq!(replaced.planes[0].counters.rx_bytes, Some(0));
    assert_eq!(
        engine.reset_traffic(&reset(&initial)).unwrap_err().code,
        ApiErrorCode::Conflict
    );
    engine.observe_host_interfaces(Some(&[]));
    assert_eq!(
        engine
            .traffic_snapshot(&TrafficGetQuery { scope: host() })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
    engine.observe_host_interfaces(Some(&[sample(7, 9)]));
    assert_ne!(stat(&engine, host()).generation, replaced.generation);
}
#[test]
fn host_event_does_not_change_legacy_sampling_or_replay_and_is_bounded() {
    let engine = engine();
    let samples: Vec<_> = (1..=130)
        .map(|i| HostInterfaceSample {
            name: format!("test{i}"),
            ..sample(i, 10)
        })
        .collect();
    engine.observe_host_interfaces(Some(&samples));
    engine.push_traffic_stats_sampled();
    engine.push_host_traffic_stats_sampled();
    engine.push_host_traffic_stats_sampled();
    engine.push_host_traffic_stats_sampled();
    let replay = engine.events_snapshot(&EventFilter::default());
    let mut host_count = 0;
    for event in replay {
        if event.event_type == event_type::STATS_SCOPES_SAMPLED {
            let page: TrafficListSnapshot = serde_json::from_value(event.payload).unwrap();
            assert!(page
                .scopes
                .iter()
                .all(|s| !matches!(s.scope, TrafficScope::HostInterface { .. })));
        } else if event.event_type == event_type::STATS_HOST_INTERFACES_SAMPLED {
            let page: TrafficListSnapshot = serde_json::from_value(event.payload).unwrap();
            assert!(page.scopes.len() <= 64);
            host_count += page.scopes.len();
        }
    }
    assert_eq!(host_count, 130);
}
#[test]
fn oversized_or_duplicate_provider_batch_does_not_accumulate_or_delete_current_labels() {
    let engine = engine();
    engine.observe_host_interfaces(Some(&[sample(7, 100)]));
    engine.observe_host_interfaces(Some(&[sample(7, 101), sample(7, 102)]));
    assert_eq!(stat(&engine, host()).planes[0].counters.rx_bytes, None);
    let oversized: Vec<_> = (1..=257)
        .map(|i| HostInterfaceSample {
            name: format!("test{i}"),
            ..sample(i, 5)
        })
        .collect();
    engine.observe_host_interfaces(Some(&oversized));
    let page = engine
        .traffic_snapshots(&TrafficListQuery {
            include_host_interfaces: true,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        page.scopes
            .iter()
            .filter(|s| matches!(s.scope, TrafficScope::HostInterface { .. }))
            .count(),
        1
    );
}

#[test]
fn selected_host_pages_preserve_sorted_unique_identities_offsets_and_missing_target_errors() {
    let engine = engine();
    engine.observe_host_interfaces(Some(&[
        HostInterfaceSample {
            name: "test-b".into(),
            ..sample(7, 100)
        },
        HostInterfaceSample {
            name: "test-a".into(),
            ..sample(8, 100)
        },
    ]));
    let a = TrafficScope::HostInterface {
        name: "test-a".into(),
    };
    let b = TrafficScope::HostInterface {
        name: "test-b".into(),
    };
    let query = TrafficListQuery {
        scopes: vec![b.clone(), a.clone(), b.clone()],
        limit: Some(1),
        ..Default::default()
    };
    let first = engine.traffic_snapshots(&query).unwrap();
    assert_eq!(first.total, 2);
    assert_eq!(first.scopes[0].scope, a);
    assert_eq!(first.next_offset, Some(1));
    let second = engine
        .traffic_snapshots(&TrafficListQuery { offset: 1, ..query })
        .unwrap();
    assert_eq!(second.total, 2);
    assert_eq!(second.scopes[0].scope, b);
    assert_eq!(second.next_offset, None);
    assert_eq!(
        engine
            .traffic_snapshots(&TrafficListQuery {
                scopes: vec![host()],
                ..Default::default()
            })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
}
