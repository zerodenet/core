use std::net::SocketAddr;
use std::sync::Arc;

use zero_dns::DnsSystem;
use zero_traits::IpAddress;

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

    pub(crate) fn outbound_datagram_socket_factory(
        &self,
    ) -> zero_transport::OutboundDatagramSocketFactory {
        zero_transport::OutboundDatagramSocketFactory::new(self.egress_interface.clone())
            .with_host_resolver(Arc::new(NodeHostResolver {
                resolver: self.resolver.clone(),
            }))
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) async fn bind_datagram_socket(
        &self,
        peer: std::net::SocketAddr,
    ) -> Result<zero_platform_tokio::TokioDatagramSocket, zero_engine::EngineError> {
        self.outbound_datagram_socket_factory()
            .bind_tokio(peer)
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug)]
struct NodeHostResolver {
    resolver: Arc<DnsSystem>,
}

impl zero_transport::OutboundHostResolver for NodeHostResolver {
    fn resolve(&self, host: String, port: u16) -> zero_transport::OutboundHostResolveFuture {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            resolver.resolve_node(&host).await.map(|addresses| {
                addresses
                    .into_iter()
                    .map(|address| SocketAddr::new(ip_address_to_std(address), port))
                    .collect()
            })
        })
    }
}

fn ip_address_to_std(address: IpAddress) -> std::net::IpAddr {
    match address {
        IpAddress::V4(octets) => std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)),
        IpAddress::V6(octets) => std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)),
    }
}
