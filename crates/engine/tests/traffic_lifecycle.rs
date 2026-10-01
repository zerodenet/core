use crate::traffic_recovery::engine;
use zero_api::*;

#[test]
fn generation_changes_reject_stale_reset_without_changing_observation_epoch() {
    let engine = engine();
    let binding = engine.config().endpoint_bindings().remove(0);
    let source = engine
        .prepare_endpoint_traffic(&binding, &["peer-1".into()])
        .unwrap();
    source
        .endpoint()
        .enable(TrafficPlane::Inner, &[TrafficMetric::RxBytes]);
    source.publish();
    let query = TrafficGetQuery {
        scope: TrafficScope::Endpoint {
            endpoint_id: binding.endpoint_id.clone(),
        },
    };
    let mut endpoint = engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: binding.endpoint_id,
        })
        .unwrap();
    endpoint.state = EndpointRuntimeState::Running;
    engine.record_endpoint_runtime_state(&endpoint);
    let before = engine.traffic_snapshot(&query).unwrap();
    endpoint.state = EndpointRuntimeState::Stopped;
    engine.record_endpoint_runtime_state(&endpoint);
    endpoint.state = EndpointRuntimeState::Running;
    engine.record_endpoint_runtime_state(&endpoint);
    let next = engine.traffic_snapshot(&query).unwrap();
    assert_ne!(before.generation, next.generation);
    assert_eq!(before.stats_epoch, next.stats_epoch);
    assert_eq!(
        engine
            .reset_traffic(&StatsResetCommand {
                expected_core_instance_id: before.core_instance_id,
                operation_id: None,
                targets: vec![StatsResetTarget {
                    scope: before.scope,
                    expected_stats_epoch: before.stats_epoch,
                    expected_generation: before.generation
                }]
            })
            .unwrap_err()
            .code,
        ApiErrorCode::Conflict
    );
    let reset = engine
        .reset_traffic(&StatsResetCommand {
            expected_core_instance_id: next.core_instance_id,
            operation_id: None,
            targets: vec![StatsResetTarget {
                scope: next.scope,
                expected_stats_epoch: next.stats_epoch,
                expected_generation: next.generation,
            }],
        })
        .unwrap();
    assert_eq!(reset.snapshots[0].generation, next.generation);
    assert_eq!(
        engine
            .endpoint_snapshot(&EndpointGetQuery {
                endpoint_id: endpoint.endpoint_id
            })
            .unwrap()
            .state,
        EndpointRuntimeState::Running
    );
}
#[test]
fn relay_membership_does_not_copy_final_flow_bytes_onto_intermediate_hops() {
    use zero_core::{Address, Network, ProtocolType, Session};
    let engine = engine();
    let mut config = (*engine.config()).clone();
    let mut hop = config.outbounds[0].clone();
    hop.tag = "middle".into();
    config.outbounds.push(hop);
    engine.reload_runtime_config(config).unwrap();
    let mut session = Session::new(
        0,
        Address::Ipv4([198, 51, 100, 10]),
        80,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut session, "source").unwrap();
    session.outbound_tag = Some("a".into());
    engine.set_session_outbound_with_path(
        &session,
        None,
        vec![("middle".into(), "wireguard".into())],
    );
    engine.record_session_outbound_tx(session.id, 123);
    let query = |tag: &str| {
        engine
            .traffic_snapshot(&TrafficGetQuery {
                scope: TrafficScope::Outbound { tag: tag.into() },
            })
            .unwrap()
    };
    assert_eq!(query("a").planes[0].counters.tx_bytes, Some(123));
    assert_eq!(query("middle").planes[0].counters.tx_bytes, Some(0));
    assert_eq!(query("middle").activity.active_stream_flows, Some(1));
    assert_eq!(query("middle").planes[2].counters.tx_bytes, None);
    let mut config = (*engine.config()).clone();
    config.endpoints.clear();
    config.inbounds.clear();
    config.outbounds.clear();
    engine.reload_runtime_config(config).unwrap();
    engine
        .finish_session(session.id, zero_engine::SessionOutcome::ChainedRelayed)
        .unwrap();
    assert_eq!(engine.stats_snapshot().bytes_up, 123);
    assert!(engine.stats_snapshot().per_outbound.is_empty());
}

#[test]
fn correlation_ids_do_not_alias_distinct_resets_or_sample_events() {
    let engine = engine();
    let query = TrafficGetQuery {
        scope: TrafficScope::Global,
    };
    for _ in 0..2 {
        let current = engine.traffic_snapshot(&query).unwrap();
        engine
            .reset_traffic(&StatsResetCommand {
                expected_core_instance_id: current.core_instance_id,
                operation_id: Some("same-correlation".into()),
                targets: vec![StatsResetTarget {
                    scope: current.scope,
                    expected_stats_epoch: current.stats_epoch,
                    expected_generation: None,
                }],
            })
            .unwrap();
    }
    for _ in 0..10 {
        engine.push_traffic_stats_sampled();
    }
    let events = engine.events_snapshot(&EventFilter {
        event_types: vec![
            event_type::STATS_RESET.into(),
            event_type::STATS_SCOPES_SAMPLED.into(),
        ],
        ..Default::default()
    });
    let ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(events.len(), 12);
    assert_eq!(ids.len(), 12);
    let epochs = events
        .iter()
        .filter(|event| event.event_type == event_type::STATS_RESET)
        .map(|event| {
            event.payload["snapshots"][0]["stats_epoch"]
                .as_str()
                .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(epochs.len(), 2);
}
