use super::*;
#[test]
fn original_error_details_and_opaque_request_id_keep_the_existing_wire() {
    let id = serde_json::json!({"same":[1,"actual",null]});
    let mut error = ApiError::new(zero_api::ApiErrorCode::Conflict, "actual result");
    error.field_path = Some("params.revision".into());
    error.details.push(zero_api::ErrorDetail::new(
        Some("params.revision"),
        "same diagnostic",
    ));
    let original = zero_api::ApiResponse::<()>::from_api_error(&error).with_id(Some(id.clone()));
    assert_eq!(
        serde_json::to_value(BorrowedErrorResponse::new(Some(&id), &error)).unwrap(),
        serde_json::to_value(original).unwrap()
    );
}
