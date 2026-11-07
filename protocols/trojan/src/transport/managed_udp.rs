use std::future::Future;

use zero_core::Session;
use zero_platform_tokio::TokioSocket;
use zero_transport::RuntimeError;
use zero_transport::TcpRelayStream;

use super::outbound::OwnedTrojanOutboundTlsPlan;

#[derive(Debug, Clone)]
pub struct TrojanManagedUdpFlowResume {
    mux_pool: crate::mux::TrojanMuxConnectionPool,
    transport: OwnedTrojanOutboundTlsPlan,
    protocol: crate::udp::PreparedTrojanUdpFlowPlan,
}

pub type TrojanManagedUdpConnectorFlow = crate::udp::TrojanUdpConnectorFlow;

impl TrojanManagedUdpFlowResume {
    pub(super) fn new(
        mux_pool: crate::mux::TrojanMuxConnectionPool,
        transport: OwnedTrojanOutboundTlsPlan,
        protocol: crate::udp::PreparedTrojanUdpFlowPlan,
    ) -> Self {
        Self {
            mux_pool,
            transport,
            protocol,
        }
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> TrojanManagedUdpConnectorFlow {
        self.protocol.connector_flow(server, port, session_id)
    }

    pub async fn open_direct_connection<OpenSocket, OpenSocketFut>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
    ) -> Result<crate::udp::TrojanUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send,
    {
        if let Some(opened) = self.open_installed_connection(session).await? {
            return Ok(opened);
        }
        let transport = self.transport.clone();
        self.protocol
            .open_udp_flow_with_transport_or_mux(
                session,
                None,
                &self.mux_pool,
                move |tls_profile| async move {
                    transport
                        .open_direct_with_profile(open_socket, tls_profile)
                        .await
                },
            )
            .await
    }

    // One erased compatibility boundary avoids embedding native acquisition in
    // every generic legacy socket caller. No additional physical driver exists.
    fn open_installed_connection<'a>(
        &'a self,
        session: &'a Session,
    ) -> zero_platform_tokio::contract::BoxFuture<
        'a,
        Result<Option<crate::udp::TrojanUdpFlowConnection>, RuntimeError>,
    > {
        Box::pin(async move {
            let Some(binding) = self.mux_pool.installed_root_binding().await? else {
                return Ok(None);
            };
            let leaf = self.clone();
            self.protocol
                .open_native_mux_flow(session, &self.mux_pool, binding, move |raw| async move {
                    leaf.transport
                        .open_owned_relay_with_profile(
                            TcpRelayStream::new(zero_transport::pooled_driver::ExclusiveIo::new(
                                raw,
                            )),
                            leaf.protocol.native_tls_profile(),
                        )
                        .await
                })
                .await
                .map(Some)
        })
    }

    pub async fn open_relay_connection(
        &self,
        stream: TcpRelayStream,
        session: &Session,
        tls_server_name: Option<&str>,
    ) -> Result<crate::udp::TrojanUdpFlowConnection, RuntimeError> {
        let transport = self.transport.clone();
        self.protocol
            .open_udp_flow_with_transport(session, tls_server_name, move |tls_profile| async move {
                transport.open_relay_with_profile(stream, tls_profile).await
            })
            .await
    }
}
