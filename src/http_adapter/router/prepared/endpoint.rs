//! Same endpoint intent/actual observer is measured and encoded synchronously.
use super::*;
pub(super) fn handles(request: &QueryRequest) -> bool {
    matches!(
        request,
        QueryRequest::Endpoints(_) | QueryRequest::Endpoint(_) | QueryRequest::EndpointDetails(_)
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
        match handle
            .control_endpoint_query(request)
            .expect("selected concrete endpoint owner")
        {
            Ok(guard) => {
                let view = guard.freeze(&mut workspace)?;
                let encoded = crate::ipc::owner::encode_response(
                    &ApiResponse::ok(view.body()),
                    budget,
                    &mut storage,
                    true,
                    false,
                );
                encoded
            }
            Err(error) => crate::ipc::owner::encode_response(
                &crate::ipc::owner::BorrowedMessageResponse::new(
                    None,
                    error.code(),
                    &error.message(),
                ),
                budget,
                &mut storage,
                true,
                false,
            ),
        }
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same endpoint output"),
    )))
}
