//! Remaining legacy tuple consumers; not a new protocol execution path.
use super::*;
#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ManagedTupleUdpResumeConnector for ::vless::transport::VlessManagedUdpFlowResume {
    type ConnectorFlow = ::vless::transport::VlessManagedUdpConnectorFlow;
    type Connection = ::vless::udp::VlessUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "vless_establish";
    const RELAY_UPSTREAM_STAGE: &'static str = "vless_relay_upstream";
    const RELAY_ESTABLISH_STAGE: &'static str = "vless_relay_establish";
    const RELAY_SEND_STAGE: &'static str = "vless_relay_send";

    fn connector_flow(&self, server: &str, port: u16, session_id: u64) -> Self::ConnectorFlow {
        ::vless::transport::VlessManagedUdpFlowResume::connector_flow(
            self, server, port, session_id,
        )
    }

    async fn open_direct(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<Self::Connection, EngineError> {
        let socket_factory = services.outbound_datagram_socket_factory();
        let ech_resolver = services.ech_resolver();
        self.open_direct_connection(
            session,
            move |server, port| {
                let services = services.clone();
                let server = server.to_owned();
                async move { services.connect_upstream_owned(server, port).await }
            },
            socket_factory,
            ech_resolver,
        )
        .await
        .map_err(EngineError::from)
    }

    async fn open_relay(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
        _tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, EngineError> {
        self.open_relay_connection(stream, session, services.ech_resolver())
            .await
            .map_err(EngineError::from)
    }
}

#[cfg(feature = "vless")]
impl From<::vless::transport::VlessManagedUdpConnectorFlow> for ManagedStreamConnectorFlow {
    fn from(flow: ::vless::transport::VlessManagedUdpConnectorFlow) -> Self {
        let (cache_key, requires_relay_upstream) = flow.into_parts();
        Self::new(cache_key, requires_relay_upstream)
    }
}

#[cfg(feature = "vless")]
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for ::vless::udp::VlessUdpFlowConnection {
    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        ::vless::udp::VlessUdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::vless::udp::VlessUdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "vless upstream closed"
    }
}
