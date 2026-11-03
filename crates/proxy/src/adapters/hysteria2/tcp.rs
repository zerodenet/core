use crate::runtime::tcp_dispatch::operation::SessionTcpHandshake;

#[async_trait::async_trait]
impl SessionTcpHandshake for ::hysteria2::transport::Hysteria2TransportLeaf {
    fn tag(&self) -> &str {
        self.tag()
    }

    fn server(&self) -> &str {
        self.server()
    }

    fn port(&self) -> u16 {
        self.port()
    }

    fn connect_stage(&self) -> &'static str {
        "connect_upstream_hysteria2"
    }

    async fn open_tcp_stream(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        let sockets = services.outbound_datagram_socket_factory();
        ::hysteria2::transport::Hysteria2TransportLeaf::open_tcp_stream(self, session, &sockets)
            .await
    }
}

#[cfg(test)]
#[path = "tcp/tests.rs"]
mod tests;
