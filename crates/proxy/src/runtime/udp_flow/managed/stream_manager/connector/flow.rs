use super::super::super::connection::SharedManagedUdpConnection;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::managed::flow::ManagedRelayStreamCarrier;
use async_trait::async_trait;
use zero_core::Session;
use zero_engine::EngineError;

#[async_trait]
pub(crate) trait ManagedStreamFlowConnector:
    Clone + Send + Sync + std::fmt::Debug + 'static
{
    const ESTABLISH_STAGE: &'static str;
    const RELAY_UPSTREAM_STAGE: &'static str;
    const RELAY_ESTABLISH_STAGE: &'static str;
    const RELAY_SEND_STAGE: &'static str;

    fn connector_flow(
        &self,
        endpoint: OutboundEndpoint,
        session_id: u64,
    ) -> ManagedStreamConnectorFlow;

    async fn establish_direct(
        &self,
        services: UdpRuntimeServices,
        session: &Session,
        endpoint: OutboundEndpoint,
    ) -> Result<SharedManagedUdpConnection, EngineError>;

    async fn establish_relay(
        &self,
        carrier: ManagedRelayStreamCarrier<'_>,
        tls_server_name: Option<&str>,
        services: Option<UdpRuntimeServices>,
        session: &Session,
        endpoint: OutboundEndpoint,
    ) -> Result<SharedManagedUdpConnection, EngineError>;
}

pub(crate) struct ManagedStreamConnectorFlow {
    cache_key: String,
    requires_relay_upstream: bool,
}

impl ManagedStreamConnectorFlow {
    pub(crate) fn new(cache_key: String, requires_relay_upstream: bool) -> Self {
        Self {
            cache_key,
            requires_relay_upstream,
        }
    }

    pub(crate) fn into_parts(self) -> (String, bool) {
        (self.cache_key, self.requires_relay_upstream)
    }
}
