//! One original history guard and the existing actual response lease.
use super::*;
pub(super) fn prepare(handle: &ProxyHandle, budget: &Arc<ControlBudget>) -> io::Result<HttpBody> {
    // Output storage must outlive history so a refund's external wake never
    // runs while holding the Engine publication/history lock.
    let mut storage = None;
    let bytes = {
        let original = handle.engine_handle().inner().original_event_history();
        let body = ApiResponse::ok(original.snapshot_body());
        crate::ipc::owner::encode_response(&body, budget, &mut storage, true, false)
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same actual event history response"),
    )))
}
