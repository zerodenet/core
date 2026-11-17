use zero_core::Address;
use zero_engine::EngineError;

#[async_trait::async_trait]
pub(crate) trait ManagedTupleUdpSender: Send + Sync {
    async fn send(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<usize, EngineError>;
}
