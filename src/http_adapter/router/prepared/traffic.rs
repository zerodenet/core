//! Actual Traffic periods/roles remain borrowed only during synchronous encode.
use super::*;
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &QueryRequest,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    let mut workspace = budget.reserve(ControlCharge::ResponseBytes(0))?;
    let mut storage = None;
    let bytes = {
        let selected = handle
            .engine_handle()
            .inner()
            .control_traffic_query(request)
            .expect("selected actual Traffic owner");
        match selected {
            Ok(guard) => {
                let view = guard.freeze(&mut workspace)?;
                crate::ipc::owner::encode_response(
                    &ApiResponse::ok(view.body()),
                    budget,
                    &mut storage,
                    true,
                    false,
                )
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
        storage.take().expect("same original Traffic response"),
    )))
}
