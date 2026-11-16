//! Remaining process-plugin UDP execution, pending its native platform provider.
use super::super::ShadowsocksAdapter;
use crate::{
    protocol_registry::UdpFlowResult,
    runtime::{
        udp_dispatch::{operation::ManagedDatagramStartPlan, FlowFailure},
        udp_flow::managed::{
            datagram_manager::{
                ManagedDatagramSocketConnectorFlow, ManagedDatagramSocketResumeConnector,
            },
            ManagedDatagramFlowConnection,
        },
    },
};
use zero_engine::EngineError;
#[async_trait::async_trait]
impl ManagedDatagramSocketResumeConnector
    for ::shadowsocks::transport::ShadowsocksManagedDatagramFlowResume
{
    type Connection = ::shadowsocks::transport::ShadowsocksUdpSocketFlow;

    const ESTABLISH_STAGE: &'static str = "ss_establish";
    const SEND_STAGE: &'static str = "ss_send";
    const RESOLVE_UPSTREAM_MESSAGE: &'static str = "failed to resolve shadowsocks udp upstream";

    fn connector_flow(
        &self,
        _endpoint: crate::runtime::path::OutboundEndpoint,
    ) -> ManagedDatagramSocketConnectorFlow {
        let spec = ::shadowsocks::transport::managed_socket_flow_from_resume(self);
        ManagedDatagramSocketConnectorFlow::new(spec.into_cache_key())
    }

    async fn open_connection(
        self,
        endpoint: std::net::SocketAddr,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<Self::Connection, EngineError> {
        ::shadowsocks::transport::establish_shadowsocks_udp_socket_flow_with_resume(
            endpoint, self, &sockets,
        )
        .await
        .map_err(EngineError::from)
    }
}

#[async_trait::async_trait]
impl ManagedDatagramFlowConnection for ::shadowsocks::transport::ShadowsocksUdpSocketFlow {
    async fn request_datagram(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<crate::runtime::udp_flow::managed::ManagedDatagramPendingResponse, EngineError>
    {
        let response = ::shadowsocks::transport::ShadowsocksUdpSocketFlow::request_datagram(
            self, target, port, payload,
        )
        .await
        .map_err(EngineError::from)?;
        Ok(Box::pin(async move {
            response.await.map_err(EngineError::from)
        }))
    }
}

impl ShadowsocksAdapter {
    pub(in crate::adapters::shadowsocks) fn udp_flow_operation<'a>(
        &self,
        leaf: ::shadowsocks::transport::ShadowsocksTransportLeaf,
    ) -> UdpFlowResult<'a> {
        let plan = leaf.udp_flow_plan().map_err(|error| FlowFailure {
            stage: "udp_shadowsocks_resume",
            error: EngineError::Io(std::io::Error::other(error.to_string())),
            upstream: Some((leaf.server().into(), leaf.port())),
        })?;
        Ok(Box::new(
            crate::runtime::udp_dispatch::operation::ManagedDatagramUdpOperation {
                plan: ManagedDatagramStartPlan::from_parts(plan.into_parts()),
            },
        ))
    }
}
