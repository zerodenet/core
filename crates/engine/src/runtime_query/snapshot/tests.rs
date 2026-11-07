use super::*;
use zero_api::EventSource;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
fn engine() -> Engine {
    Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn session(engine: &Engine) -> zero_core::Session {
    let mut session = zero_core::Session::new(
        0,
        zero_core::Address::Domain("same-雪.invalid".into()),
        443,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::new("vless"),
    );
    let mut auth = zero_core::SessionAuth::new("vless");
    auth.principal_key = Some("principal-雪".into());
    session.auth = Some(auth);
    engine.prepare_session(&mut session, "same-source").unwrap();
    session.outbound_tag = Some("direct".into());
    engine.set_session_outbound_with_remote(&session, Some(("203.0.113.7", 443)));
    engine.record_session_inbound_rx(session.id, 17);
    session
}
fn times(expected: &mut serde_json::Value, actual: &serde_json::Value) {
    match (expected, actual) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (key, value) in a {
                if ["occurred_at_unix_ms", "sampled_at_unix_ms"].contains(&key.as_str()) {
                    *value = b[key].clone();
                } else if let Some(actual) = b.get(key) {
                    times(value, actual);
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
fn original_snapshot_matches_owned_library_wire_and_never_inserts_a_replay_sequence() {
    let engine = engine();
    let _session = session(&engine);
    let handle = crate::EngineHandle::new(engine.clone());
    let legacy = handle.subscribe(Default::default()).unwrap();
    let mut expected = serde_json::to_value(legacy.try_recv().unwrap()).unwrap();
    let (live, wants_snapshot) = handle.subscribe_control_live(Default::default()).unwrap();
    assert!(wants_snapshot && live.try_recv_retained().is_none());
    let sequence = engine.latest_event_sequence();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()));
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_flow_snapshot();
        let view = guard.freeze(&mut workspace).unwrap();
        let actual = serde_json::to_value(&view).unwrap();
        expected["event_id"] = actual["event_id"].clone(); // Independent original identity, never sequence.
        times(&mut expected, &actual);
        assert_eq!(actual, expected);
        assert_eq!(serde_json::to_value(&view).unwrap(), actual);
        assert_eq!(view.sequence(), sequence);
        assert_eq!(actual["payload"]["records"][0]["traffic"]["bytes_up"], 17);
        assert_eq!(actual["payload"]["principal_flows"][0]["active_flows"], 1);
        assert_eq!(engine.latest_event_sequence(), sequence);
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    let _next = session(&engine);
    assert!(live.try_recv_retained().is_some());
}
#[test]
fn snapshot_workspace_refusal_returns_original_guards_without_allocating_a_dto() {
    let engine = engine();
    let _session = session(&engine);
    let mut limits = ControlBudgetLimits::from_runtime(&Default::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_flow_snapshot();
        assert_eq!(
            guard.freeze(&mut workspace).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    drop(workspace);
    assert_eq!(book.usage().response_bytes, 0);
    let _next = session(&engine); // Takes the same membership lock again.
}

#[test]
fn two_actual_prepaid_initial_snapshots_keep_distinct_event_ids_and_zero_publication_sequence() {
    let engine = engine();
    assert_eq!(engine.latest_event_sequence(), 0);
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()));
    // Both retained identity strings are paid before their original allocation.
    // Every actual producer/view goes away before this credit can refund.
    let mut credit = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let capture = |credit: &mut ControlBudgetPermit| {
        let guard = engine.control_flow_snapshot();
        let view = guard.freeze(credit).unwrap();
        credit
            .grow(super::measure::display_len(view.event_id()).unwrap())
            .unwrap();
        let identity = view.event_id().to_string();
        (identity, Some(view.sequence()))
    };
    let first = capture(&mut credit);
    let second = capture(&mut credit);
    assert_ne!(first.0, second.0);
    assert_eq!(first.1, Some(0));
    assert_eq!(second.1, Some(0));
    assert_eq!(engine.latest_event_sequence(), 0);
    assert_eq!(book.usage().response_bytes, first.0.len() + second.0.len());
    drop(first);
    drop(second);
    drop(credit);
    assert_eq!(book.usage().response_bytes, 0);
}
