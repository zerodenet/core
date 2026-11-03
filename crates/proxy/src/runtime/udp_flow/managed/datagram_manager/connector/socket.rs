use std::net::SocketAddr;

use async_trait::async_trait;
use zero_engine::EngineError;

use super::super::super::datagram::ManagedDatagramFlowConnection;
use crate::runtime::path::OutboundEndpoint;

pub(crate) struct ManagedDatagramSocketConnectorFlow {
    cache_key: String,
}

#[async_trait]
pub(crate) trait ManagedDatagramSocketResumeConnector:
    Clone + Send + Sync + std::fmt::Debug + 'static
{
    type Connection: ManagedDatagramFlowConnection;

    const ESTABLISH_STAGE: &'static str;
    const SEND_STAGE: &'static str;
    const RESOLVE_UPSTREAM_MESSAGE: &'static str;
    const PROXY_CONTEXT_MESSAGE: &'static str = "expected proxy context for managed datagram flow";

    fn connector_flow(&self, endpoint: OutboundEndpoint) -> ManagedDatagramSocketConnectorFlow;

    async fn open_connection(
        self,
        endpoint: SocketAddr,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<Self::Connection, EngineError>;
}

impl ManagedDatagramSocketConnectorFlow {
    pub(crate) fn new(cache_key: String) -> Self {
        Self { cache_key }
    }

    pub(crate) fn into_cache_key(self) -> String {
        self.cache_key
    }
}
