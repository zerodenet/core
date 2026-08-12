use async_trait::async_trait;
use zero_core::Session;
use zero_engine::EngineError;

use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::principal_rate_limit::TrafficRateLimiters;
use crate::transport::{SharedRateLimiter, TcpRelayStream};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MessageRelayOutcome {
    Continue,
    Close,
    Upgraded,
}

pub(crate) struct MessageRelayContext {
    services: TcpRuntimeServices,
    session_id: u64,
    rate_limiters: TrafficRateLimiters,
    idle_timeout: std::time::Duration,
}

impl MessageRelayContext {
    pub(crate) fn new(
        services: TcpRuntimeServices,
        session_id: u64,
        rate_limiters: TrafficRateLimiters,
        idle_timeout: std::time::Duration,
    ) -> Self {
        Self {
            services,
            session_id,
            rate_limiters,
            idle_timeout,
        }
    }

    pub(crate) fn upload_limiter(&self) -> Option<SharedRateLimiter> {
        self.rate_limiters.upload()
    }
    pub(crate) fn download_limiter(&self) -> Option<SharedRateLimiter> {
        self.rate_limiters.download()
    }
    pub(crate) fn idle_timeout(&self) -> std::time::Duration {
        self.idle_timeout
    }
    pub(crate) fn record_upload(&self, bytes: u64) {
        self.record_upload_io(bytes, bytes);
    }
    pub(crate) fn record_download(&self, bytes: u64) {
        self.record_download_io(bytes, bytes);
    }
    pub(crate) fn record_upload_io(&self, inbound_rx: u64, outbound_tx: u64) {
        self.services
            .record_session_inbound_rx(self.session_id, inbound_rx);
        self.services
            .record_session_outbound_tx(self.session_id, outbound_tx);
    }
    pub(crate) fn record_download_io(&self, outbound_rx: u64, inbound_tx: u64) {
        self.services
            .record_session_outbound_rx(self.session_id, outbound_rx);
        self.services
            .record_session_inbound_tx(self.session_id, inbound_tx);
    }
    pub(crate) async fn relay_bidirectional(
        &self,
        client: &mut TcpRelayStream,
        upstream: TcpRelayStream,
    ) -> Result<(), EngineError> {
        let (upstream_read, upstream_write) = tokio::io::split(upstream);
        let (client_read, client_write) = tokio::io::split(client);
        let upload_services = self.services.clone();
        let download_services = self.services.clone();
        let session_id = self.session_id;
        tokio::try_join!(
            crate::transport::copy_one_way(
                client_read,
                upstream_write,
                move |bytes| super::record_tcp_upload(&upload_services, session_id, bytes),
                self.upload_limiter(),
            ),
            crate::transport::copy_one_way(
                upstream_read,
                client_write,
                move |bytes| super::record_tcp_download(&download_services, session_id, bytes),
                self.download_limiter(),
            )
        )?;
        Ok(())
    }
}

#[async_trait]
pub(crate) trait MessageInboundProtocol: Send + Sync {
    type Request: Send + Sync;

    fn session<'a>(&self, request: &'a Self::Request) -> &'a Session;
    fn set_effective_target(
        &self,
        request: &mut Self::Request,
        target: &zero_core::Address,
        port: u16,
    );
    async fn send_blocked(&self, client: &mut TcpRelayStream) -> Result<(), EngineError>;
    async fn send_upstream_failure(&self, client: &mut TcpRelayStream) -> Result<(), EngineError>;
    async fn relay(
        &self,
        client: &mut TcpRelayStream,
        upstream: TcpRelayStream,
        request: &Self::Request,
        context: MessageRelayContext,
    ) -> Result<MessageRelayOutcome, EngineError>;
}
