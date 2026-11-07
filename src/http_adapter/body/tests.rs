use super::*;
#[test]
fn flat_query_and_borrowed_error_preserve_original_http_envelope_shapes() {
    let value = serde_json::json!({"nested":{"values":[1,"actual",null]},"unknown":true});
    let body = HttpBody::query(Ok(QueryResponse::Unknown(value.clone())));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body.bytes_for_test()).unwrap(),
        serde_json::to_value(ApiResponse::ok(value)).unwrap()
    );
    let mut error = ApiError::new(zero_api::ApiErrorCode::Conflict, "actual original failure");
    error.field_path = Some("params.expected_intent_revision".into());
    error.details.push(zero_api::ErrorDetail::new(
        Some("params.expected_intent_revision"),
        "actual structured diagnostic",
    ));
    let expected = serde_json::to_value(ApiResponse::<()>::from_api_error(&error)).unwrap();
    for body in [HttpBody::query(Err(error.clone())), HttpBody::Error(error)] {
        assert_eq!(body.len().unwrap(), body.bytes_for_test().len());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body.bytes_for_test()).unwrap(),
            expected
        );
    }
}
