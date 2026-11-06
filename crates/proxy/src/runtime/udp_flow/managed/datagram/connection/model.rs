//! One typed protocol aggregate; response registrations stay with its receiver.
use zero_core::Address;
use zero_engine::EngineError;
pub(crate) type ManagedDatagramPendingResponse =
    zero_platform_tokio::contract::BoxFuture<'static, Result<(Address, u16, Vec<u8>), EngineError>>;
#[async_trait::async_trait]
pub(crate) trait ManagedDatagramFlowConnection: Send + Sync + 'static {
    async fn request_datagram(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<ManagedDatagramPendingResponse, EngineError>;
}
