use async_trait::async_trait;
use zero_core::Address;
use zero_engine::EngineError;

use super::model::UpstreamAssociationCloseReason;
use super::target::UpstreamAssociationTarget;
use crate::protocol_registry::UdpRuntimeServices;

#[async_trait]
pub(crate) trait UpstreamAssociationTransport<T>: Send + Sync + Sized
where
    T: UpstreamAssociationTarget,
{
    async fn establish(
        services: UdpRuntimeServices,
        target: T,
        session: &zero_core::Session,
        inbound_tag: &str,
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

    fn close(self, reason: UpstreamAssociationCloseReason);
}
