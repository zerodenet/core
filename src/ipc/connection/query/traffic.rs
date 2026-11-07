//! One exact Traffic owner/period capture, prepaid before materializing output.
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
        let selected = handle
            .engine_handle()
            .inner()
            .control_traffic_query(request)
            .expect("selected actual Traffic producer");
        match selected {
            Ok(guard) => {
                let view = guard.freeze(&mut workspace)?;
                super::super::super::owner::encode_response(
                    &ApiResponse::ok_with_id(id, view),
                    budget,
                    &mut storage,
                    false,
                    true,
                )
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
        storage.take().expect("same encoded Traffic storage"),
    ))
}
