use super::*;
#[test]
fn borrowed_formatted_errors_keep_original_escaped_wire_and_correlation() {
    let tag = "original-\"雪\\";
    struct Missing<'a>(&'a str);
    impl std::fmt::Display for Missing<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "policy `{}` was not found", self.0)
        }
    }
    let missing = Missing(tag);
    let id = Some(serde_json::json!({"opaque":[2,"same"]}));
    let original = zero_api::ApiError::new(zero_api::ApiErrorCode::NotFound, missing.to_string());
    let borrowed = BorrowedMessageResponse::new(id.as_ref(), original.code, &missing);
    let mut wire = Vec::new();
    serde_json::to_writer(&mut wire, &borrowed).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&wire).unwrap(),
        serde_json::to_value(crate::ipc::owner::BorrowedErrorResponse::new(
            id.as_ref(),
            &original
        ))
        .unwrap()
    );
}
