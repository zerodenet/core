use std::marker::PhantomData;
use std::net::SocketAddr;

use async_trait::async_trait;
use zero_engine::EngineError;

use super::super::super::connection::SharedManagedDatagramUdpConnection;
use super::super::super::datagram::{
    managed_datagram_connection_from_flow, ManagedDatagramFlowConnection,
};
use super::super::manager::ManagedDatagramSocketFlowManager;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::packet_path::UdpPacketRef;

#[async_trait]
pub(crate) trait ManagedDatagramSocketFlowConnector<T>: Send + Sync {
    fn connector_flow(
        &self,
        resume: &T,
        endpoint: OutboundEndpoint,
    ) -> ManagedDatagramSocketConnectorFlow;

    async fn establish(
        &self,
        services: Option<UdpRuntimeServices>,
        endpoint: OutboundEndpoint,
        resume: T,
        initial_packet: UdpPacketRef<'_>,
    ) -> Result<SharedManagedDatagramUdpConnection, EngineError>;
}

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
    const MISMATCH_STAGE: &'static str;
    const MISMATCH_MESSAGE: &'static str;
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

struct RegisteredManagedDatagramSocketResumeConnector<T>(PhantomData<fn() -> T>);

pub(crate) fn managed_datagram_socket_handler_box<T>(
) -> Box<dyn super::super::super::model::ManagedDatagramFlowHandler>
where
    T: ManagedDatagramSocketResumeConnector,
{
    Box::new(ManagedDatagramSocketFlowManager::new(
        RegisteredManagedDatagramSocketResumeConnector::<T>(PhantomData),
        T::ESTABLISH_STAGE,
        T::SEND_STAGE,
        T::MISMATCH_STAGE,
        T::MISMATCH_MESSAGE,
    ))
}

#[async_trait]
impl<T> ManagedDatagramSocketFlowConnector<T> for RegisteredManagedDatagramSocketResumeConnector<T>
where
    T: ManagedDatagramSocketResumeConnector,
{
    fn connector_flow(
        &self,
        resume: &T,
        endpoint: OutboundEndpoint,
    ) -> ManagedDatagramSocketConnectorFlow {
        resume.connector_flow(endpoint)
    }

    async fn establish(
        &self,
        services: Option<UdpRuntimeServices>,
        endpoint: OutboundEndpoint,
        resume: T,
        _initial_packet: UdpPacketRef<'_>,
    ) -> Result<SharedManagedDatagramUdpConnection, EngineError> {
        let services = services
            .ok_or_else(|| EngineError::Io(std::io::Error::other(T::PROXY_CONTEXT_MESSAGE)))?;
        let target_addr = services
            .resolve_node_address(
                &endpoint.address(),
                endpoint.port,
                T::RESOLVE_UPSTREAM_MESSAGE,
            )
            .await?;
        let sockets = services.network().outbound_datagram_socket_factory();
        let connection = resume.open_connection(target_addr, sockets).await?;
        Ok(managed_datagram_connection_from_flow(connection))
    }
}
