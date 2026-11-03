use std::future::Future;

use zero_core::Session;
use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_transport::RuntimeError;

use super::outbound::OwnedVmessOutboundTransportPlan;

#[derive(Debug, Clone)]
pub struct VmessManagedUdpFlowResume {
    mux_pool: crate::mux::VmessMuxConnectionPool,
    protocol: crate::udp::PreparedVmessUdpFlowPlan,
    transport: OwnedVmessOutboundTransportPlan,
}

pub type VmessManagedUdpConnectorFlow = crate::udp::VmessUdpConnectorFlow;

impl VmessManagedUdpFlowResume {
    pub(super) fn new(
        mux_pool: crate::mux::VmessMuxConnectionPool,
        protocol: crate::udp::PreparedVmessUdpFlowPlan,
        transport: OwnedVmessOutboundTransportPlan,
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
    ) -> VmessManagedUdpConnectorFlow {
        self.protocol.connector_flow(server, port, session_id)
    }

    pub async fn open_direct_connection<OpenSocket, OpenSocketFut>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
    ) -> Result<crate::udp::VmessUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send,
    {
        if let Some(opened) = self.open_installed_connection(session).await? {
            return Ok(opened);
        }
        let transport = self.transport.clone();
        let direct_transport =
            || transport.open_direct(move |server, port| open_socket.clone()(server, port));
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

    // Erase once at the compatibility boundary; generic socket callers do not
    // embed the entire native pool/transport preparation state machine.
    fn open_installed_connection<'a>(
        &'a self,
        session: &'a Session,
    ) -> zero_platform_tokio::contract::BoxFuture<
        'a,
        Result<Option<crate::udp::VmessUdpFlowConnection>, RuntimeError>,
    > {
        Box::pin(async move {
            let Some(binding) = self.mux_pool.installed_root_binding().await? else {
                return Ok(None);
            };
            self.protocol
                .open_native_mux_flow(
                    session,
                    self.transport.server(),
                    self.transport.port(),
                    &self.mux_pool,
                    binding,
                    |raw| async move {
                        self.transport
                            .open_relay(TcpRelayStream::new(
                                zero_transport::pooled_driver::ExclusiveIo::new(raw),
                            ))
                            .await
                    },
                )
                .await
                .map(Some)
        })
    }

    pub async fn open_relay_connection(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<crate::udp::VmessUdpFlowConnection, RuntimeError> {
        let transport = self.transport.clone();
        self.protocol
            .open_relay_udp_flow_with_transport(session, stream, |stream| {
                transport.open_relay(stream)
            })
            .await
    }
}
