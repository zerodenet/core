//! Synchronously freeze actual rows only after Root workspace prepayment.
use super::*;
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
        let guard = handle
            .engine_handle()
            .inner()
            .control_runtime_query(request)
            .expect("selected actual Engine owner");
        let encoded = match guard.freeze(&mut workspace)? {
            Ok(view) => super::super::super::owner::encode_response(
                &ApiResponse::ok_with_id(id, view),
                budget,
                &mut storage,
                false,
                true,
            ),
            Err(missing) => {
                let message = missing.message();
                super::super::super::owner::encode_response(
                    &super::super::super::owner::BorrowedMessageResponse::new(
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
        };
        encoded
    }?;
    Ok(super::super::super::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same original encoded envelope"),
    ))
}
