//! No endpoint/config/error DTO is constructed before its actual book admits it.
use super::*;
pub(super) fn handles(request: &zero_api::QueryRequest) -> bool {
    matches!(
        request,
        zero_api::QueryRequest::Endpoints(_)
            | zero_api::QueryRequest::Endpoint(_)
            | zero_api::QueryRequest::EndpointDetails(_)
    )
}
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &zero_api::QueryRequest,
    id: Option<serde_json::Value>,
    budget: &Arc<ControlBudget>,
) -> io::Result<super::super::super::owner::EncodedResponse> {
    let mut workspace =
        budget.reserve(zero_runtime::configuration::ControlCharge::ResponseBytes(0))?;
    let mut storage = None;
    let bytes = {
        match handle
            .control_endpoint_query(request)
            .expect("selected concrete endpoint owner")
        {
            Ok(guard) => {
                let view = guard.freeze(&mut workspace)?;
                let encoded = super::super::super::owner::encode_response(
                    &ApiResponse::ok_with_id(id, view),
                    budget,
                    &mut storage,
                    false,
                    true,
                );
                encoded
            }
            Err(error) => super::super::super::owner::encode_response(
                &super::super::super::owner::BorrowedMessageResponse::new(
                    id.as_ref(),
                    error.code(),
                    &error.message(),
                ),
                budget,
                &mut storage,
                false,
                true,
            ),
        }
    }?;
    Ok(super::super::super::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same endpoint output"),
    ))
}
