#[cfg(feature = "udp-runtime")]
mod datagram;
mod root;
use std::net::SocketAddr;
use std::sync::Arc;

use zero_dns::DnsSystem;
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
    ChannelError, Reason,
};
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
    pub(crate) fn network_generation(&self) -> u64 {
        self.egress_interface.generation()
    }
    pub(crate) async fn connect_direct(
        &self,
        session: &zero_core::Session,
        policy: &zero_traits::DialPolicy,
    ) -> Result<crate::transport::DirectTcpConnection, crate::transport::DirectTcpConnectFailure>
    {
        self.connector
            .connect_with_policy(session, &self.resolver, &self.egress_interface, policy)
            .await
    }
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
    pub(crate) fn egress_control(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.egress_interface.clone()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn egress_generation(&self) -> u64 {
        self.egress_interface.generation()
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

struct NodeHostResolver {
    resolver: Arc<dyn DnsService>,
}
impl std::fmt::Debug for NodeHostResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeHostResolver")
            .finish_non_exhaustive()
    }
}

impl zero_transport::OutboundHostResolver for NodeHostResolver {
    fn resolve(&self, host: String, port: u16) -> zero_transport::OutboundHostResolveFuture {
        let resolver = self.resolver.clone();
        Box::pin(async move {
            resolver
                .resolve(
                    DnsRequest::new(host, DnsQueryRole::Node),
                    DnsQueryContext::default(),
                )
                .await
                .map(|answer| {
                    answer
                        .addresses
                        .into_iter()
                        .map(|address| SocketAddr::new(ip_address_to_std(address), port))
                        .collect()
                })
                .map_err(dns_error)
        })
    }
}

#[derive(Debug)]
struct LegacyDnsError(ChannelError);
impl std::fmt::Display for LegacyDnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::error::Error::source(&self.0) {
            Some(source) => std::fmt::Display::fmt(source, f),
            None => std::fmt::Display::fmt(&self.0, f),
        }
    }
}
impl std::error::Error for LegacyDnsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
fn dns_error(error: ChannelError) -> std::io::Error {
    let kind = match error.reason {
        Reason::Timeout => std::io::ErrorKind::TimedOut,
        Reason::Cancelled => std::io::ErrorKind::Interrupted,
        Reason::StalePlan | Reason::Revoked => std::io::ErrorKind::NotConnected,
        Reason::InvalidConfig => std::io::ErrorKind::InvalidInput,
        _ => std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .map(std::io::Error::kind)
            .unwrap_or(std::io::ErrorKind::Other),
    };
    std::io::Error::new(kind, LegacyDnsError(error))
}

fn ip_address_to_std(address: IpAddress) -> std::net::IpAddr {
    match address {
        IpAddress::V4(octets) => std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)),
        IpAddress::V6(octets) => std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets)),
    }
}
