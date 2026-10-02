use std::net::SocketAddr;
use std::sync::Arc;

use zero_dns::DnsSystem;
use zero_traits::IpAddress;

use crate::transport::DirectConnector;
#[cfg(feature = "tls-ech-runtime")]
mod ech;
#[cfg(feature = "tls-ech-runtime")]
use ech::RuntimeEchResolver;

/// Narrow network service exposed to protocol-owned connect/handshake code.
/// It deliberately carries no engine, configuration, health, or accounting registry
/// access. Optional prepared I/O observers only receive boundary facts.
#[derive(Clone)]
pub(crate) struct UpstreamConnectServices {
    pub(super) resolver: Arc<DnsSystem>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) connector: DirectConnector,
    pub(super) egress_interface: zero_platform_tokio::EgressInterfaceControl,
    #[cfg(feature = "tls-ech-runtime")]
    ech_resolver: Arc<RuntimeEchResolver>,
}

impl UpstreamConnectServices {
    pub(crate) fn with_observer(
        mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        self.observer = observer;
        self
    }
    pub(crate) fn observe_stream(
        &self,
        stream: zero_transport::TcpRelayStream,
    ) -> zero_transport::TcpRelayStream {
        match &self.observer {
            Some(observer) => zero_transport::TcpRelayStream::new(
                zero_transport::observed::ObservedStream::new(stream, observer.clone()),
            ),
            None => stream,
        }
    }
    pub(crate) fn observer(&self) -> Option<Arc<dyn zero_traits::IoObserver>> {
        self.observer.clone()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn egress_for_ip(
        &self,
        destination: std::net::IpAddr,
    ) -> Option<zero_platform_tokio::EgressInterface> {
        self.egress_interface.current_for(destination.is_ipv6())
    }

    pub(super) fn new(
        resolver: Arc<DnsSystem>,
        connector: DirectConnector,
        egress_interface: zero_platform_tokio::EgressInterfaceControl,
    ) -> Self {
        #[cfg(feature = "tls-ech-runtime")]
        let ech_resolver = Arc::new(RuntimeEchResolver::new(
            resolver.clone(),
            egress_interface.clone(),
        ));
        Self {
            resolver,
            observer: None,
            connector,
            egress_interface,
            #[cfg(feature = "tls-ech-runtime")]
            ech_resolver,
        }
    }

    pub(crate) async fn connect_upstream_owned(
        &self,
        server: String,
        port: u16,
    ) -> Result<zero_platform_tokio::TokioSocket, zero_transport::RuntimeError> {
        self.connector
            .connect_host(
                &server,
                port,
                self.resolver.as_ref(),
                &self.egress_interface,
            )
            .await
            .map(|socket| socket.with_observer(self.observer.clone()))
            .inspect_err(|_| {
                if let Some(observer) = &self.observer {
                    observer.error();
                }
            })
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
            .with_observer(self.observer.clone())
            .with_host_resolver(Arc::new(NodeHostResolver {
                resolver: self.resolver.clone(),
            }))
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn egress_generation(&self) -> u64 {
        self.outbound_datagram_socket_factory().egress_generation()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) async fn resolve_node_address(
        &self,
        address: &zero_core::Address,
        port: u16,
        error_message: &'static str,
    ) -> Result<std::net::SocketAddr, zero_engine::EngineError> {
        self.connector
            .resolve_node_address(address, port, self.resolver.as_ref(), error_message)
            .await
            .map_err(Into::into)
    }

    #[cfg(feature = "tls-ech-runtime")]
    pub(crate) fn ech_resolver(&self) -> Arc<dyn zero_transport::tls::ech::EchConfigResolver> {
        self.ech_resolver.clone()
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
