use super::super::super::connection::ManagedTupleUdpFlowConnection;
use async_trait::async_trait;
use zero_engine::EngineError;

use crate::protocol_registry::PacketPathExecutionServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::packet_path::UdpPacketRef;

pub(crate) struct ManagedDatagramConnectorFlow {
    cache_key: String,
}

#[async_trait]
pub(crate) trait ManagedDatagramResumeConnector:
    Clone + Send + Sync + std::fmt::Debug + 'static
{
    type Connection: ManagedTupleUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str;

    fn connector_flow(
        &self,
        endpoint: OutboundEndpoint,
        session_id: u64,
    ) -> ManagedDatagramConnectorFlow;

    async fn open_connection(
        self,
        services: PacketPathExecutionServices,
        endpoint: OutboundEndpoint,
        initial_packet: UdpPacketRef<'_>,
    ) -> Result<Self::Connection, EngineError>;
}

impl ManagedDatagramConnectorFlow {
    pub(crate) fn new(cache_key: String) -> Self {
        Self { cache_key }
    }

    pub(crate) fn into_cache_key(self) -> String {
        self.cache_key
    }
}
