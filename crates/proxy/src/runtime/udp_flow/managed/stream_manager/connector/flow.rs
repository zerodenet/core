use std::any::Any;

use super::super::super::connection::SharedManagedUdpConnection;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::managed::flow::ManagedRelayStreamCarrier;
use async_trait::async_trait;
use zero_core::Session;
use zero_engine::EngineError;

#[async_trait]
pub(crate) trait ManagedStreamFlowConnector:
    Any + Clone + Send + Sync + std::fmt::Debug + 'static
{
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

pub(crate) trait ManagedStreamConnectorParts {
    fn into_managed_connector_parts(self) -> (String, bool);
}

pub(crate) trait ManagedStreamConnectorFlowBuild {
    fn into_parts(self) -> (String, bool);
}

impl<T> ManagedStreamConnectorFlowBuild for T
where
    T: ManagedStreamConnectorParts,
{
    fn into_parts(self) -> (String, bool) {
        ManagedStreamConnectorParts::into_managed_connector_parts(self)
    }
}

pub(crate) fn managed_stream_connector_flow_from_build(
    build: impl ManagedStreamConnectorFlowBuild,
) -> ManagedStreamConnectorFlow {
    let (cache_key, requires_relay_upstream) = build.into_parts();
    ManagedStreamConnectorFlow::new(cache_key, requires_relay_upstream)
}
