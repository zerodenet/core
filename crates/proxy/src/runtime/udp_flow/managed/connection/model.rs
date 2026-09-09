use crate::runtime::udp_flow::packet_path::ChainTask;
use std::sync::Arc;
use tokio::task::JoinSet;
use zero_core::Address;
use zero_engine::EngineError;

#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
#[async_trait::async_trait]
pub(crate) trait ManagedUdpConnection: Send + Sync {
    fn is_closed(&self) -> bool {
        false
    }
    async fn send(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<usize, EngineError>;

    fn spawn_response_bridge(&self, chain_tasks: &mut JoinSet<ChainTask>, session_id: u64);
}

#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) type SharedManagedUdpConnection = Arc<dyn ManagedUdpConnection>;

#[cfg(feature = "managed-datagram-runtime")]
#[async_trait::async_trait]
pub(crate) trait ManagedDatagramUdpConnection: Send + Sync {
    async fn send_datagram(
        &self,
        chain_tasks: &mut JoinSet<ChainTask>,
        session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError>;
}

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) type SharedManagedDatagramUdpConnection = Arc<dyn ManagedDatagramUdpConnection>;
