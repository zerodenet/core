use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_engine::EngineError;

use super::accounting::{record_tcp_download, record_tcp_upload};
use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::principal_rate_limit::TrafficRateLimiters;
use crate::transport::{relay_bidirectional_metered_throttled, TcpRelayStream};

#[async_trait]
pub(crate) trait InboundProtocol: Send + Sync {
    type ClientStream: AsyncRead + AsyncWrite + Unpin + Send;

    async fn send_ok(&self, client: &mut Self::ClientStream) -> Result<(), EngineError>;

    async fn send_blocked(&self, client: &mut Self::ClientStream) -> Result<(), EngineError>;

    async fn send_upstream_failure(
        &self,
        client: &mut Self::ClientStream,
    ) -> Result<(), EngineError>;

    async fn relay(
        &self,
        client: Self::ClientStream,
        upstream: TcpRelayStream,
        services: TcpRuntimeServices,
        session_id: u64,
        rate_limiters: TrafficRateLimiters,
    ) -> Result<(), EngineError> {
        let upload_services = services.clone();
        let download_services = services;
        let result = relay_bidirectional_metered_throttled(
            client,
            upstream,
            move |bytes| {
                record_tcp_upload(&upload_services, session_id, bytes);
            },
            move |bytes| {
                record_tcp_download(&download_services, session_id, bytes);
            },
            rate_limiters.upload(),
            rate_limiters.download(),
        )
        .await;
        drop(rate_limiters);
        result.map(|_| ()).map_err(EngineError::Io)
    }
}
