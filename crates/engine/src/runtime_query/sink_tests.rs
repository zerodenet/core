//! Status serialization borrows the actual dispatcher-published status record.
use super::*;
use zero_api::QueryService;
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits, ControlCharge};
#[test]
fn same_sink_guard_preserves_every_existing_optional_field_and_releases_before_update() {
    let engine = Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let status:zero_api::SinkStatus=serde_json::from_value(serde_json::json!({
 "name":"same-sink-雪","pending":7,"total_delivered":3,"total_failed":2,"replay_gaps":1,
 "last_success_at_unix_ms":Some(11),"last_failure_at_unix_ms":Some(12),"last_error":"original \"status\"",
 "delivery":{"in_flight":true,"retry_pending":2,"ack_retry_pending":3,"durable_pending":4,"next_retry_at_unix_ms":17}
 })).unwrap();
    engine.update_sink_status(vec![status.clone()]);
    let request = QueryRequest::Sinks(Default::default());
    let expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    {
        let guard = engine.control_runtime_query(&request).unwrap();
        let view = guard.freeze(&mut workspace).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&view).unwrap(), expected);
        assert_eq!(
            book.usage().response_bytes,
            0,
            "sink view allocates no query workspace"
        );
    }
    drop(workspace);
    let mut changed = status;
    changed.pending = 8;
    changed.last_error = None;
    engine.update_sink_status(vec![changed]);
    let expected = serde_json::to_value(engine.query(request.clone()).unwrap()).unwrap();
    let mut workspace = book.reserve(ControlCharge::ResponseBytes(0)).unwrap();
    let guard = engine.control_runtime_query(&request).unwrap();
    let view = guard.freeze(&mut workspace).unwrap().unwrap();
    assert_eq!(serde_json::to_value(view).unwrap(), expected);
}
