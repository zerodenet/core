use super::*;
#[test]
fn actual_proxy_status_http_get_and_typed_entry_keep_same_flat_wire_after_unlock() {
    let (handle, book) = fixture();
    let request = QueryRequest::TunStatus(zero_api::TunStatusQuery);
    let expected = handlers::serialize_query(handle.query(request.clone()))
        .unwrap()
        .bytes_for_test();
    let original = prepare(&handle, Selected::TunStatus, &book).unwrap();
    assert_eq!(original.bytes_for_test(), expected);
    assert_eq!(book.usage().response_bytes, original.len().unwrap());
    assert!(
        handle.query(request.clone()).is_ok(),
        "encoded bytes do not keep the actual Proxy status lock"
    );
    drop(original);
    assert_eq!(book.usage().response_bytes, 0);
    let input = serde_json::to_vec(&request).unwrap();
    let typed = prepare(&handle, Selected::Typed(&input), &book).unwrap();
    assert_eq!(typed.bytes_for_test(), expected);
    drop(typed);
    assert_eq!(book.usage().response_bytes, 0);
    assert_eq!(book.usage().request_bytes, 0);
}
