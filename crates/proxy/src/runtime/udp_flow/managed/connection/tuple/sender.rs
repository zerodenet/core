use zero_core::Address;
use zero_engine::EngineError;

#[async_trait::async_trait]
pub(crate) trait ManagedTupleUdpSender: Send + Sync {
    fn is_closed(&self) -> bool {
        false
    }
    async fn send(&self, target: &Address, port: u16, payload: &[u8])
        -> Result<usize, EngineError>;

    fn subscribe_responses(&self) -> tokio::sync::broadcast::Receiver<(Address, u16, Vec<u8>)>;

    fn closed_message(&self) -> &'static str;
}
