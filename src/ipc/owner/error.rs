//! Both control wires borrow the original ApiError and correlation ID.
use serde::Serialize;
use zero_api::ApiError;
#[derive(Serialize)]
pub(crate) struct BorrowedErrorResponse<'a> {
    api_id: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a serde_json::Value>,
    ok: bool,
    error: Envelope<'a>,
}
#[derive(Serialize)]
struct Envelope<'a> {
    code: &'static str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    field_path: Option<&'a str>,
    #[serde(skip_serializing_if = "empty")]
    details: &'a [zero_api::ErrorDetail],
}
impl<'a> BorrowedErrorResponse<'a> {
    pub(crate) fn new(id: Option<&'a serde_json::Value>, error: &'a ApiError) -> Self {
        Self {
            api_id: zero_api::API_ID,
            id,
            ok: false,
            error: Envelope {
                code: error.code.as_code_str(),
                message: &error.message,
                field_path: error.field_path.as_deref(),
                details: &error.details,
            },
        }
    }
}
fn empty(details: &&[zero_api::ErrorDetail]) -> bool {
    details.is_empty()
}

#[cfg(test)]
mod tests;
