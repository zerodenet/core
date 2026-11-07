//! Actual Proxy owner guards survive only synchronous premeasure/encode.
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
    let bytes = if matches!(request, zero_api::QueryRequest::Health(_)) {
        let view = handle.control_health_query(&mut workspace)?;
        super::super::super::owner::encode_response(
            &ApiResponse::ok_with_id(id, view),
            budget,
            &mut storage,
            false,
            true,
        )
    } else {
        let view = handle
            .control_proxy_query(request)
            .expect("selected concrete Proxy status owner");
        super::super::super::owner::encode_response(
            &ApiResponse::ok_with_id(id, view),
            budget,
            &mut storage,
            false,
            true,
        )
    }?;
    Ok(super::super::super::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same original Proxy status envelope"),
    ))
}
