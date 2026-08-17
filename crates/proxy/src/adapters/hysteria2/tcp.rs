use crate::adapters::hysteria2::Hysteria2Adapter;
use crate::protocol_registry::{claim_session_tcp_leaf, ClaimedTcpOutboundLeaf};
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

impl Hysteria2Adapter {
    pub(super) fn claim_tcp_outbound_leaf_impl<'a>(
        &self,
        leaf: ::hysteria2::transport::Hysteria2TransportLeaf,
    ) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> {
        claim_session_tcp_leaf(leaf)
    }
}
