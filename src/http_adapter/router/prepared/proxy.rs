//! Same actual Proxy status owner, without constructing an owned DTO first.
use super::*;
pub(super) fn prepare(
    handle: &ProxyHandle,
    request: &QueryRequest,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    let mut storage = None;
    let bytes = {
        let view = handle
            .control_proxy_query(request)
            .expect("selected concrete Proxy status owner");
        crate::ipc::owner::encode_response(
            &ApiResponse::ok(view.body()),
            budget,
            &mut storage,
            true,
            false,
        )
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same actual Proxy status envelope"),
    )))
}
