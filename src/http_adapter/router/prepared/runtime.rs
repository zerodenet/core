//! One real immutable/config and mutable-owner generation through encoding.
use super::*;
pub(super) fn handles(request: &QueryRequest) -> bool {
    matches!(
        request,
        QueryRequest::Sinks(_)
            | QueryRequest::PrincipalFlows(_)
            | QueryRequest::Runtime(_)
            | QueryRequest::Stats(_)
            | QueryRequest::Diagnostics(_)
            | QueryRequest::ActiveFlows(_)
            | QueryRequest::RecentFlows(_)
            | QueryRequest::Flow(_)
    )
}
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &QueryRequest,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    let mut workspace = budget.reserve(ControlCharge::ResponseBytes(0))?;
    let mut storage = None;
    let bytes = {
        let guard = handle
            .engine_handle()
            .inner()
            .control_runtime_query(request)
            .expect("selected actual Engine owner");
        let encoded = match guard.freeze(&mut workspace)? {
            Ok(view) => crate::ipc::owner::encode_response(
                &ApiResponse::ok(view.body()),
                budget,
                &mut storage,
                true,
                false,
            ),
            Err(missing) => {
                let message = missing.message();
                crate::ipc::owner::encode_response(
                    &crate::ipc::owner::BorrowedMessageResponse::new(
                        None,
                        zero_api::ApiErrorCode::NotFound,
                        &message,
                    ),
                    budget,
                    &mut storage,
                    true,
                    false,
                )
            }
        };
        encoded
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same original encoded envelope"),
    )))
}
