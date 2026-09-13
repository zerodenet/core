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
    preconnect: Option<super::runtime::preconnect::Access>,
}

pub type VlessManagedUdpConnectorFlow = crate::udp::VlessUdpConnectorFlow;

impl VlessManagedUdpFlowResume {
    pub(super) fn new(
        mux_pool: crate::mux_pool::MuxConnectionPool,
        protocol: crate::udp::PreparedVlessUdpFlowPlan,
        transport: OwnedVlessOutboundTransportPlan,
        preconnect: Option<super::runtime::preconnect::Access>,
    ) -> Self {
        Self {
            mux_pool,
            protocol,
            transport,
            preconnect,
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
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<crate::udp::VlessUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync + 'static,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send + 'static,
    {
        let transport = self.transport.clone();
        let preconnect = self.preconnect.clone();
        let direct_transport = || {
            let finish = transport.clone();
            let open = move || {
                let open_socket = open_socket.clone();
                let mut transport = transport.clone();
                let socket_factory = socket_factory.clone();
                let ech_resolver = ech_resolver.clone();
                async move {
                    transport.prepare_ech(ech_resolver.as_ref()).await?;
                    transport
                        .open_direct_unencrypted(
                            move |server, port| open_socket.clone()(server, port),
                            socket_factory,
                        )
                        .await
                }
            };
            async move {
                let stream = match preconnect {
                    Some(pool) => pool.take(open).await,
                    None => open().await,
                }?;
                finish.encrypt(stream).await
            }
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
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<crate::udp::VlessUdpFlowConnection, RuntimeError> {
        let mut transport = self.transport.clone();
        transport.prepare_ech(ech_resolver.as_ref()).await?;
        self.protocol
            .open_relay_udp_flow_with_transport(session, stream, |stream| {
                transport.open_relay(stream)
            })
            .await
    }
}
