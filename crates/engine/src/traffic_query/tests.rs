use super::*;
use std::sync::Arc;
use zero_api::{
    QueryService, TrafficGetQuery, TrafficListQuery, TrafficMetric, TrafficPlane, TrafficScope,
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
fn engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn budget() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ))
}
fn times(expected: &mut serde_json::Value, actual: &serde_json::Value) {
    match (expected, actual) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (key, value) in a {
                if [
                    "sampled_at_unix_ms",
                    "capture_started_at_unix_ms",
                    "sampled_at_monotonic_ns",
                ]
                .contains(&key.as_str())
                {
                    *value = b[key].clone();
                } else if let Some(actual) = b.get(key) {
                    times(value, actual);
                }
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            for (a, b) in a.iter_mut().zip(b) {
                times(a, b)
            }
        }
        _ => {}
    }
}
fn compare(engine: &Engine, request: QueryRequest) {
    let mut expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let book = budget();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_traffic_query(&request).unwrap().unwrap();
        let view = guard.freeze(&mut workspace).unwrap();
        let actual = serde_json::to_value(&view).unwrap();
        times(&mut expected, &actual);
        assert_eq!(actual, expected);
        assert_eq!(
            serde_json::to_value(view.body()).unwrap(),
            actual.as_object().unwrap().values().next().unwrap().clone()
        );
    }
    assert!(book.usage().response_bytes > 0);
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
}
#[test]
fn actual_traffic_scope_period_masks_host_and_selected_pagination_keep_original_wire() {
    let engine = engine();
    let scope = TrafficScope::Outbound {
        tag: "direct".into(),
    };
    let meter = engine.traffic_meter(&scope).unwrap();
    meter.enable(
        TrafficPlane::Inner,
        &[TrafficMetric::RxBytes, TrafficMetric::Errors],
    );
    meter.received(TrafficPlane::Inner, 17);
    meter.dropped_reason(TrafficPlane::Inner, zero_api::TrafficDropReason::QueueFull);
    meter.observes_role(TrafficPlane::Inner, true);
    engine.observe_host_interfaces(Some(&[crate::HostInterfaceSample {
        name: "nic-雪".into(),
        index: 7,
        accounting_basis: "same_actual_test_provider",
        counters: [Some(3); 12],
        sampled_at_unix_ms: 1234,
    }]));
    for scope in [
        TrafficScope::Global,
        scope.clone(),
        TrafficScope::HostInterface {
            name: "nic-雪".into(),
        },
    ] {
        compare(
            &engine,
            QueryRequest::TrafficStat(TrafficGetQuery { scope }),
        );
    }
    for query in [
        TrafficListQuery::default(),
        TrafficListQuery {
            include_host_interfaces: true,
            ..Default::default()
        },
        TrafficListQuery {
            scopes: vec![scope.clone(), TrafficScope::Global, scope],
            offset: 1,
            limit: Some(1),
            ..Default::default()
        },
        TrafficListQuery {
            offset: usize::MAX,
            ..Default::default()
        },
    ] {
        compare(&engine, QueryRequest::TrafficStats(query));
    }
}
#[test]
fn actual_activity_and_mutable_counters_are_frozen_once_until_original_guards_return() {
    let engine = engine();
    let mut session = zero_core::Session::new(
        0,
        zero_core::Address::Ipv4([127, 0, 0, 1]),
        80,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::UNKNOWN,
    );
    engine.prepare_session(&mut session, "source-雪").unwrap();
    session.outbound_tag = Some("direct".into());
    engine.set_session_outbound_with_remote(&session, None);
    let scope = TrafficScope::Outbound {
        tag: "direct".into(),
    };
    let meter = engine.traffic_meter(&scope).unwrap();
    meter.enable(TrafficPlane::Inner, &[TrafficMetric::RxBytes]);
    meter.received(TrafficPlane::Inner, 13);
    let request = QueryRequest::TrafficStats(Default::default());
    let book = budget();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_traffic_query(&request).unwrap().unwrap();
        let view = guard.freeze(&mut workspace).unwrap();
        let first = serde_json::to_value(&view).unwrap();
        meter.received(TrafficPlane::Inner, 19);
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            first,
            "same selected counter scalars cannot grow between measure/encode"
        );
        let global = first["traffic_stats"]["scopes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["scope"]["kind"] == "global")
            .unwrap();
        assert_eq!(global["activity"]["active_stream_flows"], 1);
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    engine
        .track_session(session.id)
        .finish(crate::SessionOutcome::Success);
    compare(&engine, request);
}
#[test]
fn workspace_rejection_returns_all_real_guards_and_static_preconditions_keep_old_errors() {
    let engine = engine();
    let initial = engine
        .traffic_snapshot(&TrafficGetQuery {
            scope: TrafficScope::Global,
        })
        .unwrap();
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let request = QueryRequest::TrafficStats(Default::default());
    {
        let guard = engine.control_traffic_query(&request).unwrap().unwrap();
        assert_eq!(
            guard.freeze(&mut workspace).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    // Same actual period owner must be available; memory credit is not native proof.
    engine
        .reset_traffic(&zero_api::StatsResetCommand {
            expected_core_instance_id: initial.core_instance_id.clone(),
            operation_id: None,
            targets: vec![zero_api::StatsResetTarget {
                scope: initial.scope,
                expected_stats_epoch: initial.stats_epoch,
                expected_generation: initial.generation,
            }],
        })
        .unwrap();
    for request in [
        QueryRequest::TrafficStat(TrafficGetQuery {
            scope: TrafficScope::Outbound {
                tag: "missing".into(),
            },
        }),
        QueryRequest::TrafficStats(TrafficListQuery {
            expected_core_instance_id: Some("other".into()),
            ..Default::default()
        }),
        QueryRequest::TrafficStats(TrafficListQuery {
            expected_registry_revision: Some(u64::MAX),
            ..Default::default()
        }),
    ] {
        let expected = engine.query(request.clone()).unwrap_err();
        let actual = engine.control_traffic_query(&request).unwrap().unwrap_err();
        assert_eq!(actual.code(), expected.code);
        assert_eq!(actual.message(), expected.message);
    }
}
