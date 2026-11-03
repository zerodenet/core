use super::super::super::connection::{
    managed_packet_udp_connection_from_flow, SharedManagedUdpConnection,
};
use super::flow::{ManagedStreamConnectorFlow, ManagedStreamFlowConnector};
use super::ManagedPacketUdpResume;
use crate::protocol_registry::{UdpRuntimeServices, UpstreamConnectServices};
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_flow::managed::flow::ManagedRelayStreamCarrier;
use crate::transport::TcpRelayStream;
use async_trait::async_trait;
use zero_core::Session;
use zero_engine::EngineError;

#[async_trait]
pub(crate) trait ManagedPacketUdpResumeConnector:
    Clone + Send + Sync + std::fmt::Debug + 'static
{
    type ConnectorFlow: Into<ManagedStreamConnectorFlow>;
    type Connection: super::super::super::connection::ManagedPacketUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str;
    const RELAY_UPSTREAM_STAGE: &'static str;
    const RELAY_ESTABLISH_STAGE: &'static str;
    const RELAY_SEND_STAGE: &'static str;

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> Self::ConnectorFlow;

    async fn open_direct(
        &self,
        services: UpstreamConnectServices,
        session: &Session,
    ) -> Result<Self::Connection, EngineError>;

    async fn open_relay(
        &self,
        stream: TcpRelayStream,
        session: &Session,
        tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError>;

    async fn open_lazy_relay(
        &self,
        carrier: LazyTcpRelayCarrier<'_>,
        session: &Session,
        tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError> {
        self.open_relay(carrier.open().await?, session, tls_server_name)
            .await
    }
}

#[async_trait]
impl<T> ManagedStreamFlowConnector for ManagedPacketUdpResume<T>
where
    T: ManagedPacketUdpResumeConnector,
{
    const ESTABLISH_STAGE: &'static str = T::ESTABLISH_STAGE;
    const RELAY_UPSTREAM_STAGE: &'static str = T::RELAY_UPSTREAM_STAGE;
    const RELAY_ESTABLISH_STAGE: &'static str = T::RELAY_ESTABLISH_STAGE;
    const RELAY_SEND_STAGE: &'static str = T::RELAY_SEND_STAGE;
    fn connector_flow(
        &self,
        endpoint: OutboundEndpoint,
        session_id: u64,
    ) -> ManagedStreamConnectorFlow {
        self.0
            .connector_flow(&endpoint.server, endpoint.port, session_id)
            .into()
    }

    async fn establish_direct(
        &self,
        services: UdpRuntimeServices,
        session: &Session,
        _endpoint: OutboundEndpoint,
    ) -> Result<SharedManagedUdpConnection, EngineError> {
        let connection = self.0.open_direct(services.upstream(), session).await?;
        Ok(managed_packet_udp_connection_from_flow(connection))
    }

    async fn establish_relay(
        &self,
        carrier: ManagedRelayStreamCarrier<'_>,
        tls_server_name: Option<&str>,
        _services: Option<UdpRuntimeServices>,
        session: &Session,
        _endpoint: OutboundEndpoint,
    ) -> Result<SharedManagedUdpConnection, EngineError> {
        let connection = match carrier {
            ManagedRelayStreamCarrier::Ready(carrier) => {
                self.0
                    .open_relay(carrier.stream, session, tls_server_name)
                    .await?
            }
            ManagedRelayStreamCarrier::Lazy(carrier) => {
                self.0
                    .open_lazy_relay(*carrier, session, tls_server_name)
                    .await?
            }
        };
        Ok(managed_packet_udp_connection_from_flow(connection))
    }
}
