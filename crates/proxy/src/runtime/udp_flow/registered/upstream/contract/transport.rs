use async_trait::async_trait;
use zero_core::Address;
use zero_engine::EngineError;

use super::model::UpstreamAssociationCloseReason;
use super::target::UpstreamAssociationTarget;
use crate::protocol_registry::{TcpExecutionServices, UdpAssociationObserver};

#[async_trait]
pub(crate) trait UpstreamAssociationTransport<T>: Send + Sync + Sized
where
    T: UpstreamAssociationTarget,
{
    async fn establish(
        execution: TcpExecutionServices,
        observer: UdpAssociationObserver,
        target: T,
    ) -> Result<Self, EngineError>;

    async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError>;

    async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), EngineError>;

    fn request_close(&mut self, reason: UpstreamAssociationCloseReason);

    /// Consume the actual retained controller's complete receipt. Cancellation
    /// or custody release cannot authorize replacement.
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, EngineError>;
}
