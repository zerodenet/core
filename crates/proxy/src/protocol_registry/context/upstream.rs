use std::sync::Arc;

use zero_dns::DnsSystem;

use crate::inventory::ProtocolInventory;

/// Narrow network service exposed to protocol-owned connect/handshake code.
/// It deliberately carries no engine, configuration, health, or accounting
/// access.
#[derive(Clone)]
pub(crate) struct UpstreamConnectServices {
    pub(super) resolver: Arc<DnsSystem>,
    pub(super) protocols: ProtocolInventory,
    pub(super) egress_interface: zero_platform_tokio::EgressInterfaceControl,
}

impl UpstreamConnectServices {
    pub(super) fn new(
        resolver: Arc<DnsSystem>,
        protocols: ProtocolInventory,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> Self {
        Self {
            resolver,
            protocols,
            egress_interface,
        }
    }

    pub(crate) async fn connect_upstream_owned(
        &self,
        server: String,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError> {
        self.protocols
            .direct_connector()
            .connect_host(
                &server,
                port,
                self.resolver.as_ref(),
                &self.egress_interface,
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn connect_upstream(
        &self,
        server: &str,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError> {
        self.connect_upstream_owned(server.to_owned(), port).await
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) async fn bind_datagram_socket(
        &self,
        peer: std::net::SocketAddr,
    ) -> Result<zero_platform_tokio::TokioDatagramSocket, zero_engine::EngineError> {
        crate::runtime::udp_socket::bind_datagram_socket_for_peer(
            peer,
            self.egress_interface.current_for(peer.is_ipv6()).as_ref(),
        )
        .await
    }
}
