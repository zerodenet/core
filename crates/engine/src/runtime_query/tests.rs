use super::*;
use zero_api::QueryService;
use zero_core::{Address, Network, ProtocolType, Session};
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
fn flow(engine: &Engine) -> Session {
    let mut session = Session::new(
        0,
        Address::Domain("example-雪.invalid".into()),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    engine.prepare_session(&mut session, "source-雪").unwrap();
    session.outbound_tag = Some("direct".into());
    engine.set_session_outbound_with_remote(&session, Some(("203.0.113.7", 443)));
    engine.record_session_inbound_rx(session.id, 17);
    engine.record_session_outbound_tx(session.id, 11);
    engine.record_session_outbound_rx(session.id, 23);
    engine.record_session_inbound_tx(session.id, 19);
    engine.record_session_network(
        session.id,
        crate::FlowNetworkObservation {
            local_address: Some(crate::FlowRemoteEndpoint {
                host: "127.0.0.1".into(),
                port: 1234,
            }),
            resolved_candidates: vec![crate::FlowRemoteEndpoint {
                host: "203.0.113.7".into(),
                port: 443,
            }],
            connection_attempts: vec![crate::FlowConnectionAttemptObservation {
                remote_address: crate::FlowRemoteEndpoint {
                    host: "203.0.113.7".into(),
                    port: 443,
                },
                local_address: None,
                stage: "connect".into(),
                outcome: "connected".into(),
                interface_bound: false,
                error_kind: None,
                os_error: None,
                error: None,
            }],
            ..Default::default()
        },
    );
    session
}
fn compare(engine: &Engine, request: QueryRequest) {
    let expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let book = budget();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let guard = engine.control_runtime_query(&request).unwrap();
    let view = guard.freeze(&mut workspace).unwrap().unwrap();
    let actual = serde_json::to_value(&view).unwrap();
    // The old DTO samples wall time again; compare all other original wire fields.
    // Copy only that single time into expected, never counters/field values.
    let mut expected = expected;
    fn timestamps(expected: &mut serde_json::Value, actual: &serde_json::Value) {
        match (expected, actual) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                for (key, value) in a.iter_mut() {
                    if key == "sampled_at_unix_ms" {
                        *value = b[key].clone();
                    } else if let Some(actual) = b.get(key) {
                        timestamps(value, actual);
                    }
                }
            }
            (serde_json::Value::Array(a), serde_json::Value::Array(b)) => {
                for (a, b) in a.iter_mut().zip(b) {
                    timestamps(a, b);
                }
            }
            _ => {}
        }
    }
    timestamps(&mut expected, &actual);
    assert_eq!(actual, expected);
    assert_eq!(
        &serde_json::to_value(view.body()).unwrap(),
        actual.as_object().unwrap().values().next().unwrap()
    );
}
#[test]
fn same_real_active_completed_network_and_legacy_wire_fields_are_borrowed() {
    let engine = engine();
    let session = flow(&engine);
    for request in [
        QueryRequest::Runtime(Default::default()),
        QueryRequest::Stats(Default::default()),
        QueryRequest::Diagnostics(Default::default()),
        QueryRequest::ActiveFlows(zero_api::FlowListQuery {
            limit: Some(1),
            filter: Default::default(),
        }),
        QueryRequest::Flow(zero_api::FlowGetQuery {
            flow_id: session.id.to_string(),
        }),
    ] {
        compare(&engine, request);
    }
    engine.finish_session_with_observation(
        session.id,
        crate::SessionOutcome::Failed,
        Some("same close 雪".into()),
        Some(crate::FlowFailureObservation {
            stage: "read".into(),
            code: Some("closed".into()),
            message: "original typed observation".into(),
            remote: Some(crate::FlowRemoteEndpoint {
                host: "203.0.113.7".into(),
                port: 443,
            }),
        }),
    );
    for request in [
        QueryRequest::Runtime(Default::default()),
        QueryRequest::RecentFlows(zero_api::FlowListQuery {
            limit: None,
            filter: Default::default(),
        }),
        QueryRequest::Flow(zero_api::FlowGetQuery {
            flow_id: session.id.to_string(),
        }),
    ] {
        compare(&engine, request);
    }
}
#[test]
fn mutable_statistics_are_frozen_once_while_the_original_atomic_counters_keep_advancing() {
    let engine = engine();
    let request = QueryRequest::Stats(Default::default());
    let book = budget();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        let view = guard.freeze(&mut workspace).unwrap().unwrap();
        let first = serde_json::to_vec(&view).unwrap();
        engine.record_udp_upstream_association_created();
        assert_eq!(serde_json::to_vec(&view).unwrap(), first);
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let guard = engine.control_runtime_query(&request).unwrap();
    let view = guard.freeze(&mut workspace).unwrap().unwrap();
    let actual = serde_json::to_value(view.body()).unwrap();
    assert_eq!(actual["udp_upstream"]["created_associations"], 1);
}
#[test]
fn workspace_refusal_leaves_no_guards_or_byte_debt_and_flow_errors_borrow_the_same_request() {
    let engine = engine();
    let session = flow(&engine);
    let request = QueryRequest::Runtime(Default::default());
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        assert_eq!(
            guard.freeze(&mut workspace).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    engine.record_session_upload(session.id, 1);
    engine.finish_session(session.id, crate::SessionOutcome::DirectRelayed);
    let request = QueryRequest::Flow(zero_api::FlowGetQuery {
        flow_id: "01\"雪".into(),
    });
    let book = budget();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let expected_error = engine.query(request.clone()).unwrap_err();
    let guard = engine.control_runtime_query(&request).unwrap();
    let missing = guard.freeze(&mut workspace).unwrap().unwrap_err();
    assert_eq!(missing.message().to_string(), "flow `01\"雪` was not found");
    assert_eq!(
        missing.message_len(),
        Some(missing.message().to_string().len())
    );
    assert_eq!(missing.into_api_error(), expected_error);
}
