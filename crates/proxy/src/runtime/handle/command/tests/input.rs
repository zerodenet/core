use super::*;
fn book() -> Arc<ControlBudget> {
    ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits::from_runtime(&Default::default()),
    )
}
#[test]
fn canonical_input_retains_exact_wire_storage_until_the_original_string_is_freed() {
    let value = serde_json::json!({"route":{"final":{"type":"direct"}},"unicode":"雪","array":[1,true,"\n"]});
    let expected = serde_json::to_string(&value).unwrap();
    let book = book();
    let raw = serialize(&value, &book).unwrap();
    assert_eq!(raw.as_ref(), expected);
    assert_eq!(book.usage().request_bytes, expected.len());
    let mut shrink = book.limits();
    shrink.messages.request_bytes_total = expected.len() - 1;
    assert!(book.prepare(shrink).is_err());
    drop(raw);
    assert_eq!(book.usage().request_bytes, 0);
    book.prepare(shrink).unwrap().commit().unwrap();
    let error = serialize(&value, &book).err().unwrap();
    assert_eq!(error.code, zero_api::ApiErrorCode::Conflict);
    assert_eq!(book.usage().request_bytes, 0);
}
