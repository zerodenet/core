use super::*;
fn book() -> Arc<ControlBudget> {
    ControlBudget::new(ControlBudgetLimits::from_runtime(&Default::default()))
}
#[test]
fn measured_storage_covers_real_value_nodes_rows_strings_and_recursive_raw_value() {
    let book = book();
    for raw in [
        r#"{"one":1}"#,
        r#"["a","b",{"k":[1,2,3]}]"#,
        r#"{"$serde_json::private::RawValue":"{\"a\":[1,2,3]}"}"#,
    ] {
        let bytes =
            control_json_storage_bound(raw.as_bytes(), &book, ControlJsonStorage::Value).unwrap();
        assert!(bytes > raw.len());
        let original: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert!(!original.is_null());
        assert_eq!(book.usage().request_bytes, 0);
    }
    assert!(control_json_typed_array_bound(usize::MAX, 8).is_err());
    assert!(control_json_map_bound(usize::MAX).is_err());
}
#[test]
fn malformed_escaped_prefix_and_measurement_workspace_are_bounded_before_parsing() {
    let raw = br#"{"s":"\nunterminated"#;
    let book = book();
    assert!(control_json_decoder_workspace(raw).unwrap() >= 2 * b"\\nunterminated".len());
    assert!(serde_json::from_slice::<serde_json::Value>(raw).is_err());
    let mut limits = book.limits();
    limits.messages.request_bytes_total = raw.len();
    book.prepare(limits).unwrap().commit().unwrap();
    assert!(
        matches!(control_json_storage_bound(raw,&book,ControlJsonStorage::Value),Err(ref e) if e.kind()==io::ErrorKind::WouldBlock)
    );
    assert_eq!(book.usage().request_bytes, 0);
}
