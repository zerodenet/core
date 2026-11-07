use super::*;
use zero_api::{TrafficGetQuery, TrafficMetric, TrafficPlane, TrafficScope};
fn times(a: &mut serde_json::Value, b: &serde_json::Value) {
    match (a, b) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (k, v) in a {
                if [
                    "sampled_at_unix_ms",
                    "capture_started_at_unix_ms",
                    "sampled_at_monotonic_ns",
                ]
                .contains(&k.as_str())
                {
                    *v = b[k].clone()
                } else if let Some(b) = b.get(k) {
                    times(v, b)
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
#[test]
fn traffic_ipc_prepaid_actual_view_keeps_nested_wire_and_byte_credit_after_owner_unlock() {
    let handle = handle();
    let book = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    let scope = TrafficScope::Outbound {
        tag: "direct".into(),
    };
    let meter = handle
        .engine_handle()
        .inner()
        .traffic_meter(&scope)
        .unwrap();
    meter.enable(TrafficPlane::Inner, &[TrafficMetric::RxBytes]);
    meter.received(TrafficPlane::Inner, 17);
    let query = QueryRequest::TrafficStat(TrafficGetQuery { scope });
    assert!(handles(&query));
    let id = Some(serde_json::json!({"same":["雪",7]}));
    let mut expected = serde_json::to_value(ApiResponse::ok_with_id(
        id.clone(),
        handle.query(query.clone()).unwrap(),
    ))
    .unwrap();
    let original = prepare(&handle, &query, id, &book).unwrap();
    let actual = serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap();
    times(&mut expected, &actual);
    assert_eq!(expected, actual);
    assert_eq!(book.usage().response_bytes, original.bytes().len());
    let mut target = book.limits();
    target.messages.response_bytes_total = original.bytes().len() - 1;
    assert!(
        book.prepare(target).is_err(),
        "actual held bytes prohibit shrink"
    );
    meter.received(TrafficPlane::Inner, 19);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(original.bytes()).unwrap(),
        actual
    );
    // Actual Engine locks have returned while the same encoded bytes remain held.
    let zero_api::QueryResponse::TrafficStat(updated) = handle.query(query).unwrap() else {
        panic!("actual traffic producer changed wire kind");
    };
    assert_eq!(
        updated
            .planes
            .iter()
            .find(|p| p.plane == TrafficPlane::Inner)
            .unwrap()
            .counters
            .rx_bytes,
        Some(36)
    );
    drop(original);
    assert_eq!(book.usage().response_bytes, 0);
}
#[test]
fn traffic_workspace_pressure_is_typed_local_capacity_and_keeps_no_owner_guard_or_byte_debt() {
    let handle = handle();
    let mut limits =
        ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default());
    limits.messages.response_bytes_total = 1;
    let book = ControlBudget::new(limits);
    let query = QueryRequest::TrafficStats(Default::default());
    let error = prepare(&handle, &query, None, &book).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(input::is_invalid_input_frame(&error));
    assert_eq!(
        error
            .get_ref()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(book.usage().response_bytes, 0);
    assert!(handle.query(query).is_ok());
}
