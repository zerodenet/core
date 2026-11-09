//! Proxy publication receipt; the generic host owns its lifetime.
use std::sync::Arc;
use tokio::sync::oneshot;
pub(crate) struct PendingReloadAck {
    pub(super) expected: Arc<zero_engine::EngineRuntimeSnapshot>,
    pub(super) previous: Arc<zero_engine::EngineRuntimeSnapshot>,
    pub(super) compensating: bool,
    pub(super) wire: Option<super::handle::configuration::wire_budget::WireCandidate>,
    pub(super) sender: oneshot::Sender<Result<(), String>>,
}

impl std::fmt::Debug for PendingReloadAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingReloadAck")
            .field("expected_revision", &self.expected.config_revision())
            .field("previous_revision", &self.previous.config_revision())
            .field("compensating", &self.compensating)
            .field(
                "wire_metadata_committed",
                &self
                    .wire
                    .as_ref()
                    .map(|wire| wire.policy().metadata_committed()),
            )
            .field("receiver_closed", &self.sender.is_closed())
            .finish_non_exhaustive()
    }
}
