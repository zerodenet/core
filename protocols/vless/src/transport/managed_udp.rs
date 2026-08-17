use std::future::Future;

use zero_core::Session;
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_transport::RuntimeError;

use super::outbound::OwnedVlessOutboundTransportPlan;

#[derive(Debug, Clone)]
pub struct VlessManagedUdpFlowResume {
    mux_pool: crate::mux_pool::MuxConnectionPool,
    protocol: crate::udp::PreparedVlessUdpFlowPlan,
    transport: OwnedVlessOutboundTransportPlan,
}

pub type VlessManagedUdpConnectorFlow = crate::udp::VlessUdpConnectorFlow;

impl VlessManagedUdpFlowResume {
    pub(super) fn new(
        mux_pool: crate::mux_pool::MuxConnectionPool,
        protocol: crate::udp::PreparedVlessUdpFlowPlan,
        transport: OwnedVlessOutboundTransportPlan,
    ) -> Self {
        Self {
            mux_pool,
            protocol,
            transport,
        }
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> VlessManagedUdpConnectorFlow {
        self.protocol.connector_flow(server, port, session_id)
    }

    pub async fn open_direct_connection<OpenSocket, OpenSocketFut>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
        socket_factory: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<crate::udp::VlessUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send,
    {
        let transport = self.transport.clone();
        let direct_transport = || {
            transport.open_direct(
                move |server, port| open_socket.clone()(server, port),
                socket_factory.clone(),
            )
        };
        self.protocol
            .open_udp_flow_with_transport_or_mux(
                session,
                self.transport.server(),
                self.transport.port(),
                &self.mux_pool,
                direct_transport,
            )
            .await
    }

    pub async fn open_relay_connection(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<crate::udp::VlessUdpFlowConnection, RuntimeError> {
        let transport = self.transport.clone();
        self.protocol
            .open_relay_udp_flow_with_transport(session, stream, |stream| {
                transport.open_relay(stream)
            })
            .await
    }
}
