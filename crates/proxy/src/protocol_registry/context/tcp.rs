use std::sync::Arc;

use zero_config::RuntimeConfig;
use zero_dns::DnsSystem;
use zero_engine::Engine;

use super::{OutboundAdapterContext, UpstreamConnectServices};
use crate::inventory::ProtocolInventory;
use crate::runtime::principal_rate_limit::{PrincipalRateLimitRegistry, TrafficRateLimiters};

#[derive(Clone)]
pub(crate) struct TcpRuntimeServices {
    pub(super) engine: Engine,
    config: Arc<RuntimeConfig>,
    pub(super) upstream: UpstreamConnectServices,
    principal_rate_limits: PrincipalRateLimitRegistry,
}

impl TcpRuntimeServices {
    pub(crate) fn new(
        engine: Engine,
        config: Arc<RuntimeConfig>,
        resolver: Arc<DnsSystem>,
        protocols: ProtocolInventory,
        principal_rate_limits: PrincipalRateLimitRegistry,
    ) -> Self {
        Self {
            engine,
            config,
            upstream: UpstreamConnectServices::new(resolver, protocols),
            principal_rate_limits,
        }
    }

    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(crate) fn config(&self) -> &RuntimeConfig {
        self.config.as_ref()
    }

    pub(crate) fn resolver(&self) -> &DnsSystem {
        self.upstream.resolver.as_ref()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn protocols(&self) -> &ProtocolInventory {
        &self.upstream.protocols
    }

    pub(crate) fn upstream(&self) -> UpstreamConnectServices {
        self.upstream.clone()
    }

    pub(crate) fn traffic_rate_limiters(
        &self,
        session: &zero_core::Session,
    ) -> TrafficRateLimiters {
        self.principal_rate_limits.acquire(session)
    }

    pub(crate) async fn connect_upstream_owned(
        &self,
        server: String,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError> {
        self.upstream.connect_upstream_owned(server, port).await
    }

    pub(crate) async fn connect_direct(
        &self,
        session: &zero_core::Session,
    ) -> Result<(zero_platform_tokio::TokioSocket, std::net::SocketAddr), zero_engine::EngineError>
    {
        self.upstream
            .protocols
            .direct_connector()
            .connect(session, self.upstream.resolver.as_ref())
            .await
            .map_err(Into::into)
    }

    pub(crate) fn check_outbound_health(&self, tag: &str) -> Result<(), zero_engine::EngineError> {
        self.engine.check_outbound_health(tag)
    }

    pub(crate) fn record_outbound_failure(&self, tag: &str) {
        self.engine.record_outbound_failure(tag);
    }

    pub(crate) fn record_outbound_success(&self, tag: &str) {
        self.engine.record_outbound_success(tag);
    }

    pub(crate) fn prepare_tcp_outbound<'a>(
        &'a self,
        resolved: &'a zero_engine::ResolvedOutbound<'a>,
    ) -> Result<crate::inventory::PreparedTcpOutbound<'a>, crate::transport::TcpOutboundFailure>
    {
        self.upstream
            .protocols
            .prepare_tcp_outbound(OutboundAdapterContext::new(self.config.as_ref()), resolved)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
        &self,
        prepared: crate::inventory::PreparedTcpRelayChain<'_>,
    ) -> Result<crate::transport::RelayCarrier, crate::transport::TcpOutboundFailure> {
        crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_carrier(
            self.clone(),
            prepared,
        )
        .await
    }

    pub(crate) fn record_control_traffic(
        &self,
        session_id: u64,
        traffic: zero_transport::StreamTraffic,
    ) {
        if traffic.is_empty() {
            return;
        }
        self.record_session_outbound_rx(session_id, traffic.read_bytes);
        self.record_session_outbound_tx(session_id, traffic.written_bytes);
    }

    pub(crate) fn record_session_inbound_rx(&self, session_id: u64, bytes: u64) {
        self.engine.record_session_inbound_rx(session_id, bytes);
    }

    pub(crate) fn record_session_inbound_tx(&self, session_id: u64, bytes: u64) {
        self.engine.record_session_inbound_tx(session_id, bytes);
    }

    pub(crate) fn record_session_outbound_rx(&self, session_id: u64, bytes: u64) {
        self.engine.record_session_outbound_rx(session_id, bytes);
    }

    pub(crate) fn record_session_outbound_tx(&self, session_id: u64, bytes: u64) {
        self.engine.record_session_outbound_tx(session_id, bytes);
    }
}
