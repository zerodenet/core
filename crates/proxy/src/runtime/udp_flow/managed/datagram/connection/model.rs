use std::sync::Arc;

use super::super::response::{ManagedDatagramResponse, ManagedDatagramResponseWaiters};
use zero_core::Address;
use zero_engine::EngineError;

#[async_trait::async_trait]
pub(crate) trait ManagedDatagramSender: Send + Sync {
    async fn send_datagram(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError>;
}

pub(super) struct ManagedDatagramConnection {
    pub(super) sender: Arc<dyn ManagedDatagramSender>,
    pub(super) waiters: ManagedDatagramResponseWaiters,
    pub(super) _response_sink: Option<Arc<dyn Fn(ManagedDatagramResponse) + Send + Sync>>,
    pub(super) closed_message: &'static str,
}

#[async_trait::async_trait]
pub(crate) trait ManagedDatagramFlowConnection: Send + Sync + 'static {
    async fn send_datagram(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError>;

    /// Only a flow's actual receive driver may implement direct delivery.
    /// The cache retains this callback for exactly the same response waiters.
    fn install_response_sink(
        &self,
        _: &Arc<dyn Fn(ManagedDatagramResponse) + Send + Sync>,
    ) -> Option<Result<(), EngineError>> {
        None
    }

    fn subscribe_responses(&self) -> tokio::sync::broadcast::Receiver<ManagedDatagramResponse>;

    fn closed_message(&self) -> &'static str;
}
