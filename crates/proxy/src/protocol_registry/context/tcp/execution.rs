//! Execution inputs that reusable relay factories may retain without a registry.
use super::super::UpstreamConnectServices;
use crate::runtime::principal_rate_limit::{PrincipalRateLimitRegistry, TrafficRateLimiters};
use std::sync::Arc;
use zero_config::RuntimeConfig;
use zero_dns::DnsSystem;
use zero_engine::{Engine, EngineRuntimeSnapshot};

#[derive(Clone)]
pub(crate) struct TcpExecutionServices {
    pub(in crate::protocol_registry::context) engine: Engine,
    pub(super) snapshot: Arc<EngineRuntimeSnapshot>,
    pub(in crate::protocol_registry::context) upstream: UpstreamConnectServices,
    pub(super) principal_rate_limits: PrincipalRateLimitRegistry,
}
impl TcpExecutionServices {
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(crate) fn config(&self) -> &RuntimeConfig {
        self.snapshot.config().as_ref()
    }

    pub(crate) fn snapshot(&self) -> &EngineRuntimeSnapshot {
        self.snapshot.as_ref()
    }

    pub(crate) fn resolver(&self) -> &DnsSystem {
        self.upstream.resolver.as_ref()
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
    ) -> Result<crate::transport::DirectTcpConnection, crate::transport::DirectTcpConnectFailure>
    {
        self.upstream
            .connector
            .connect(
                session,
                &self.upstream.resolver,
                &self.upstream.egress_interface,
            )
            .await
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
