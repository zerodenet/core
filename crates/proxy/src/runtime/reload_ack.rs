//! Proxy publication receipt; the generic host owns its lifetime.
use std::sync::Arc;
use tokio::sync::oneshot;
#[derive(Debug)]
pub(crate) struct PendingReloadAck {
    pub(super) expected: Arc<zero_engine::EngineRuntimeSnapshot>,
    pub(super) previous: Arc<zero_engine::EngineRuntimeSnapshot>,
    pub(super) compensating: bool,
    pub(super) sender: oneshot::Sender<Result<(), String>>,
}
