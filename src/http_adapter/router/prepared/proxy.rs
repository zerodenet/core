//! Same actual Proxy status owner, without constructing an owned DTO first.
use super::*;
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &QueryRequest,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    let mut workspace = budget.reserve(ControlCharge::ResponseBytes(0))?;
    let mut storage = None;
    let bytes = if matches!(request, QueryRequest::Capabilities(_)) {
        let view = handle.control_capabilities_query(&mut workspace)?;
        let encoded = crate::ipc::owner::encode_response(
            &ApiResponse::ok(view.body()),
            budget,
            &mut storage,
            true,
            false,
        );
        encoded
    } else if matches!(request, QueryRequest::Health(_)) {
        let view = handle.control_health_query(&mut workspace)?;
        let encoded = crate::ipc::owner::encode_response(
            &ApiResponse::ok(view.body()),
            budget,
            &mut storage,
            true,
            false,
        );
        encoded
    } else {
        let view = handle
            .control_proxy_query(request)
            .expect("selected concrete Proxy status owner");
        let encoded = crate::ipc::owner::encode_response(
            &ApiResponse::ok(view.body()),
            budget,
            &mut storage,
            true,
            false,
        );
        encoded
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same actual Proxy status envelope"),
    )))
}
