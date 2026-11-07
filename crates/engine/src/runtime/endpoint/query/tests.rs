//! Actual original metadata/period/role owners; no native/Scope qualification.
use super::*;
use zero_api::{QueryService, TrafficMetric, TrafficPlane, TrafficScope};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
fn config() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[{"tag":"same-雪","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":0},
            "protocol":{"type":"wireguard","private_key":"0101010101010101010101010101010101010101010101010101010101010101","addresses":["10.0.0.1/32"],
                "peers":[{"public_key":"de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f","endpoint":"127.0.0.1:51820","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
fn book() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()))
}
fn times(expected: &mut serde_json::Value, actual: &serde_json::Value) {
    match (expected, actual) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (k, v) in a {
                if k == "observed_at_unix_ms" {
                    *v = b[k].clone();
                } else if let Some(actual) = b.get(k) {
                    times(v, actual);
                }
            }
        }
        (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
            for (a, b) in a.iter_mut().zip(b) {
                times(a, b);
            }
        }
        _ => {}
    }
}
#[test]
fn same_endpoint_binding_facts_errors_recovery_and_wire_are_borrowed_after_prepayment() {
    let engine = Engine::new(config()).unwrap();
    let id = "endpoint:same-雪";
    let baseline = engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: id.into(),
        })
        .unwrap();
    assert_eq!(baseline.state, EndpointRuntimeState::Stopped); // No started resource claimed.
    engine.record_endpoint_runtime_state(&baseline);
    engine.record_endpoint_runtime_error(id, r#"same "metadata-only" failure 雪"#, true);
    engine.record_endpoint_recovery(
        id,
        zero_api::EndpointRecovery {
            network_generation: 3,
            phase: zero_api::EndpointRecoveryPhase::Retrying,
            observed_at_unix_ms: 7,
            retry_after_ms: Some(19),
            error: Some("same retained retry diagnostic".into()),
        },
    );
    for request in [
        QueryRequest::Endpoints(Default::default()),
        QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: id.into(),
        }),
    ] {
        let mut expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
        let book = book();
        let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
        {
            let guard = engine.control_endpoint_query(&request).unwrap().unwrap();
            let view = guard.freeze(&mut workspace).unwrap();
            assert!(book.usage().response_bytes > 0);
            let row = view.rows().next().unwrap();
            assert!(std::ptr::eq(
                row.binding(),
                &guard.snapshot.endpoint_intents.entries[id].binding
            ));
            let original = guard
                .facts
                .get(id)
                .unwrap()
                .last_error
                .as_ref()
                .unwrap()
                .message
                .as_ptr();
            assert_eq!(
                row.fact
                    .unwrap()
                    .last_error
                    .as_ref()
                    .unwrap()
                    .message
                    .as_ptr(),
                original
            );
            let actual = serde_json::to_value(&view).unwrap();
            times(&mut expected, &actual);
            assert_eq!(actual, expected);
            assert_eq!(
                serde_json::to_value(view.body()).unwrap(),
                actual.as_object().unwrap().values().next().unwrap().clone()
            );
        }
        drop(workspace);
        assert_eq!(book.usage().response_bytes, 0);
    }
}
#[test]
fn actual_atomic_endpoint_counters_are_frozen_once_and_packet_flow_fields_remain_distinct() {
    let engine = Engine::new(config()).unwrap();
    let id = "endpoint:same-雪";
    let meter = engine
        .traffic_meter(&TrafficScope::Endpoint {
            endpoint_id: id.into(),
        })
        .unwrap();
    meter.enable(
        TrafficPlane::Inner,
        &[TrafficMetric::RxBytes, TrafficMetric::RxPackets],
    );
    meter.received(TrafficPlane::Inner, 17);
    let route = meter.packet_route();
    let request = QueryRequest::Endpoint(EndpointGetQuery {
        endpoint_id: id.into(),
    });
    let book = book();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_endpoint_query(&request).unwrap().unwrap();
        let view = guard.freeze(&mut workspace).unwrap();
        let original = serde_json::to_value(&view).unwrap();
        assert_eq!(original["endpoint"]["counters"]["inner_rx_bytes"], 17);
        assert_eq!(original["endpoint"]["counters"]["active_packet_routes"], 1);
        meter.received(TrafficPlane::Inner, 19);
        drop(route);
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            original,
            "the same frozen scalar rows cannot change between measurement and encoding"
        );
    }
    drop(workspace);
    let expected = engine
        .endpoint_snapshot(&EndpointGetQuery {
            endpoint_id: id.into(),
        })
        .unwrap();
    assert_eq!(expected.counters.inner_rx_bytes, Some(36));
    assert_eq!(expected.counters.active_packet_routes, Some(0));
}
#[test]
fn original_empty_page_and_missing_error_allocate_no_rows_and_capacity_refusal_releases_guards() {
    let engine = Engine::new(config()).unwrap();
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let request = QueryRequest::Endpoints(Default::default());
    {
        let guard = engine.control_endpoint_query(&request).unwrap().unwrap();
        assert_eq!(
            guard.freeze(&mut workspace).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    assert_eq!(book.usage().response_bytes, 0);
    let request = QueryRequest::Endpoints(EndpointListQuery {
        offset: usize::MAX,
        limit: Some(1),
    });
    {
        let guard = engine.control_endpoint_query(&request).unwrap().unwrap();
        let rows = guard.freeze(&mut workspace).unwrap();
        assert_eq!(rows.rows().count(), 0);
        assert_eq!(rows.total(), 1);
        assert_eq!(rows.next_offset(), None);
        assert_eq!(book.usage().response_bytes, 0);
    }
    let request = QueryRequest::Endpoint(EndpointGetQuery {
        endpoint_id: r#"missing-"雪"#.into(),
    });
    let missing = engine
        .control_endpoint_query(&request)
        .unwrap()
        .unwrap_err();
    assert_eq!(missing.code(), zero_api::ApiErrorCode::NotFound);
    assert_eq!(
        missing.message().to_string(),
        r#"endpoint `missing-"雪` was not found"#
    );
    assert!(engine
        .configuration_source_owner()
        .observation()
        .reason
        .is_none());
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
}
