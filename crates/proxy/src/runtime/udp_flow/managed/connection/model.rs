use std::sync::Arc;
use zero_core::Address;
use zero_engine::EngineError;

#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
#[async_trait::async_trait]
pub(crate) trait ManagedUdpConnection: Send + Sync {
    async fn send(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<usize, EngineError>;
}

#[cfg(all(
    test,
    any(
        feature = "managed-stream-runtime",
        feature = "managed-datagram-runtime"
    )
))]
pub(crate) type SharedManagedUdpConnection = Arc<dyn ManagedUdpConnection>;
