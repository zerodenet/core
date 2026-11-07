//! Config/policy queries serialize their actual borrowed Engine generation.
use super::*;
pub(super) fn handles(request: &zero_api::QueryRequest) -> bool {
    matches!(
        request,
        zero_api::QueryRequest::Config(_)
            | zero_api::QueryRequest::Policies(_)
            | zero_api::QueryRequest::Policy(_)
            | zero_api::QueryRequest::Runtime(_)
            | zero_api::QueryRequest::Stats(_)
            | zero_api::QueryRequest::Diagnostics(_)
            | zero_api::QueryRequest::ActiveFlows(_)
            | zero_api::QueryRequest::RecentFlows(_)
            | zero_api::QueryRequest::Flow(_)
    )
}
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &zero_api::QueryRequest,
    id: Option<serde_json::Value>,
    budget: &Arc<ControlBudget>,
) -> io::Result<super::super::owner::EncodedResponse> {
    // Declared before every borrowed guard: refund notifications, including
    // serialization failure/panic, can only run after all Engine locks leave.
    prepare_original(handle, request, id, budget).map_err(input::budget)
}
fn prepare_original(
    handle: &ProxyHandle,
    request: &zero_api::QueryRequest,
    id: Option<serde_json::Value>,
    budget: &Arc<ControlBudget>,
) -> io::Result<super::super::owner::EncodedResponse> {
    if !matches!(
        request,
        zero_api::QueryRequest::Config(_)
            | zero_api::QueryRequest::Policies(_)
            | zero_api::QueryRequest::Policy(_)
    ) {
        return runtime::prepare(handle, request, id, budget);
    }
    let mut storage = None;
    let bytes = {
        let snapshot = handle.engine_handle().inner().runtime_snapshot();
        let selected = snapshot
            .control_config_query(request)
            .expect("selected Engine query");
        match selected {
            Ok(view) => super::super::owner::encode_response(
                &ApiResponse::ok_with_id(id, view),
                budget,
                &mut storage,
                false,
                true,
            ),
            Err(missing) => {
                let message = missing.message();
                super::super::owner::encode_response(
                    &super::super::owner::BorrowedMessageResponse::new(
                        id.as_ref(),
                        zero_api::ApiErrorCode::NotFound,
                        &message,
                    ),
                    budget,
                    &mut storage,
                    false,
                    true,
                )
            }
        }
    }?;
    Ok(super::super::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same encoded response storage"),
    ))
}

#[cfg(test)]
mod tests;

mod runtime;
