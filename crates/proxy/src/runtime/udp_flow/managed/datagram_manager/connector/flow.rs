use std::marker::PhantomData;

use async_trait::async_trait;
use zero_engine::EngineError;

use super::super::super::connection::{
    managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
    SharedManagedUdpConnection,
};
use super::super::manager::ManagedDatagramFlowManager;
use crate::protocol_registry::{UdpNetworkServices, UdpRuntimeServices};
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::packet_path::UdpPacketRef;

#[async_trait]
pub(crate) trait ManagedDatagramFlowConnector<T>: Send + Sync {
    const INITIAL_PACKET_PRE_SENT: bool;

    fn connector_flow(
        &self,
        resume: &T,
        endpoint: OutboundEndpoint,
    ) -> ManagedDatagramConnectorFlow;

    async fn establish(
        &self,
        services: Option<UdpRuntimeServices>,
        endpoint: OutboundEndpoint,
        resume: T,
        initial_packet: UdpPacketRef<'_>,
    ) -> Result<SharedManagedUdpConnection, EngineError>;
}

pub(crate) struct ManagedDatagramConnectorFlow {
    cache_key: String,
}

#[async_trait]
pub(crate) trait ManagedDatagramResumeConnector:
    Clone + Send + Sync + std::fmt::Debug + 'static
{
    type Connection: ManagedTupleUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str;
    const MISMATCH_STAGE: &'static str;
    const MISMATCH_MESSAGE: &'static str;

    fn connector_flow(&self, endpoint: OutboundEndpoint) -> ManagedDatagramConnectorFlow;

    async fn open_connection(
        self,
        services: UdpNetworkServices,
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

struct RegisteredManagedDatagramResumeConnector<T>(PhantomData<fn() -> T>);

pub(crate) fn managed_datagram_handler_box<T>(
) -> Box<dyn super::super::super::model::ManagedDatagramFlowHandler>
where
    T: ManagedDatagramResumeConnector,
{
    Box::new(ManagedDatagramFlowManager::new(
        RegisteredManagedDatagramResumeConnector::<T>(PhantomData),
        T::ESTABLISH_STAGE,
        T::MISMATCH_STAGE,
        T::MISMATCH_MESSAGE,
    ))
}

#[async_trait]
impl<T> ManagedDatagramFlowConnector<T> for RegisteredManagedDatagramResumeConnector<T>
where
    T: ManagedDatagramResumeConnector,
{
    const INITIAL_PACKET_PRE_SENT: bool = true;

    fn connector_flow(
        &self,
        resume: &T,
        endpoint: OutboundEndpoint,
    ) -> ManagedDatagramConnectorFlow {
        resume.connector_flow(endpoint)
    }

    async fn establish(
        &self,
        services: Option<UdpRuntimeServices>,
        endpoint: OutboundEndpoint,
        resume: T,
        initial_packet: UdpPacketRef<'_>,
    ) -> Result<SharedManagedUdpConnection, EngineError> {
        let services = services.ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "expected proxy context for managed datagram flow",
            ))
        })?;
        let connection = resume
            .open_connection(services.network(), endpoint, initial_packet)
            .await?;
        Ok(managed_tuple_udp_connection_from_flow(connection))
    }
}
