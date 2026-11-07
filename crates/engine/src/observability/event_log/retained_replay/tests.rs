use super::*;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
#[test]
fn actual_history_replay_prepays_before_cloning_and_keeps_same_payload_after_eviction_until_return()
{
    let log = EngineEventLog::default();
    let event = ApiEvent::new(
        "same-original",
        event_type::ENGINE_WARNING,
        1,
        json!({"message": "雪".repeat(8192)}),
    );
    let pointer = event.payload["message"].as_str().unwrap().as_ptr();
    log.push_external(event);
    let expected = log.events_since(0, 256, &EventFilter::default());
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 64;
    let budget = ControlBudget::new(limits);
    let result = log.retained_replay(
        0,
        256,
        &EventFilter::default(),
        budget.reserve(ControlCharge::ResponseBytes(0)).unwrap(),
    );
    assert!(matches!(result, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock));
    assert_eq!(budget.usage().response_bytes, 0);
    assert_eq!(
        log.inner.lock().unwrap()[0].payload["message"]
            .as_str()
            .unwrap()
            .as_ptr(),
        pointer
    );
    limits.messages.response_bytes_total = 128 * 1024;
    budget.prepare(limits).unwrap().commit().unwrap();
    let original = log
        .retained_replay(
            0,
            256,
            &EventFilter::default(),
            budget.reserve(ControlCharge::ResponseBytes(0)).unwrap(),
        )
        .unwrap();
    assert_eq!(original.len(), expected.events.len());
    assert_eq!(original.actual_from, expected.actual_from);
    assert_eq!(original.has_gap, expected.has_gap);
    assert_eq!(
        original.event(0).payload["message"]
            .as_str()
            .unwrap()
            .as_ptr(),
        pointer
    );
    assert_eq!(
        serde_json::to_value(original.event(0)).unwrap(),
        serde_json::to_value(&expected.events[0]).unwrap()
    );
    log.set_capacity(0);
    assert!(log.inner.lock().unwrap().is_empty());
    assert_eq!(
        original.event(0).payload["message"]
            .as_str()
            .unwrap()
            .as_ptr(),
        pointer
    );
    let charged = budget.usage().response_bytes;
    assert!(charged > 24 * 1024);
    limits.messages.response_bytes_total = charged - 1;
    assert!(
        budget.prepare(limits).is_err(),
        "eviction cannot refund a still-retained response original"
    );
    drop(original);
    assert_eq!(budget.usage().response_bytes, 0);
    budget.prepare(limits).unwrap().commit().unwrap();
    budget.close_admission();
    budget.confirm_released().unwrap();
}
