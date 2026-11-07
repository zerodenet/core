//! Known query/decoder errors borrow their actual message producer directly.
//! serde_json 1.0.154 collect_str streams escaping to its real writer; no
//! intermediate formatted String is created before response admission.
use serde::Serialize;
#[derive(Serialize)]
#[serde(bound(serialize = ""))]
pub(crate) struct BorrowedMessageResponse<'a, M: std::fmt::Display> {
    api_id: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a serde_json::Value>,
    ok: bool,
    error: MessageError<'a, M>,
}
#[derive(Serialize)]
#[serde(bound(serialize = ""))]
struct MessageError<'a, M: std::fmt::Display> {
    code: &'static str,
    message: Message<'a, M>,
}
struct Message<'a, M: std::fmt::Display>(&'a M);
impl<M: std::fmt::Display> Serialize for Message<'_, M> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self.0)
    }
}
impl<'a, M: std::fmt::Display> BorrowedMessageResponse<'a, M> {
    pub(crate) fn new(
        id: Option<&'a serde_json::Value>,
        code: zero_api::ApiErrorCode,
        message: &'a M,
    ) -> Self {
        Self {
            api_id: zero_api::API_ID,
            id,
            ok: false,
            error: MessageError {
                code: code.as_code_str(),
                message: Message(message),
            },
        }
    }
}
#[cfg(test)]
mod tests;
