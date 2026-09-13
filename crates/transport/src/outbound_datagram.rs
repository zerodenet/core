use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use zero_platform_tokio::{EgressInterfaceControl, TokioDatagramSocket};

/// Owned future returned by the runtime node resolver bridge.
pub type OutboundHostResolveFuture =
    Pin<Box<dyn Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static>>;

/// Runtime bridge used by UDP/QUIC carriers to resolve their node endpoint
/// without reaching around the configured DNS subsystem.
pub trait OutboundHostResolver: Send + Sync + std::fmt::Debug {
    fn resolve(&self, host: String, port: u16) -> OutboundHostResolveFuture;
}

/// Runtime-owned factory for every external UDP/QUIC socket.
///
/// The control handle and node resolver are read when a socket is created, so
/// route and DNS reconciliation affect new flows without mutating established
/// sockets.
#[derive(Clone, Debug)]
pub struct OutboundDatagramSocketFactory {
    egress: EgressInterfaceControl,
    final_mask: crate::finalmask::Profile,
    resolver: Option<Arc<dyn OutboundHostResolver>>,
    relay: Option<RelayFactory>,
    hopping: Option<crate::datagram_hop::Profile>,
}

impl OutboundDatagramSocketFactory {
    pub fn with_final_mask(mut self, profile: crate::finalmask::Profile) -> Self {
        self.final_mask = profile;
        self
    }
    pub fn final_mask(&self) -> &crate::finalmask::Profile {
        &self.final_mask
    }

    pub fn new(egress: EgressInterfaceControl) -> Self {
        Self {
            egress,
            final_mask: Default::default(),
            resolver: None,
            relay: None,
            hopping: None,
        }
    }

    pub fn with_host_resolver(mut self, resolver: Arc<dyn OutboundHostResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    pub async fn resolve_server_addresses(
        &self,
        host: &str,
        port: u16,
    ) -> io::Result<Vec<SocketAddr>> {
        if let Ok(address) = host.parse::<std::net::IpAddr>() {
            return Ok(vec![SocketAddr::new(address, port)]);
        }
        let resolver = self.resolver.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("node host `{host}` requires the runtime DNS resolver"),
            )
        })?;
        let addresses = resolver.resolve(host.to_owned(), port).await?;
        let mut deduplicated = Vec::with_capacity(addresses.len());
        for address in addresses {
            if !deduplicated.contains(&address) {
                deduplicated.push(address);
            }
        }
        if deduplicated.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("node host `{host}` resolved to no addresses"),
            ));
        }
        Ok(deduplicated)
    }

    pub fn egress_for(&self, peer: SocketAddr) -> Option<zero_platform_tokio::EgressInterface> {
        self.egress.current_for_peer(peer)
    }

    pub fn egress_generation(&self) -> u64 {
        self.egress.generation()
    }

    pub fn bind_std(&self, peer: SocketAddr) -> io::Result<std::net::UdpSocket> {
        self.require_direct()?;
        let interface = self.egress.try_current_for_peer(peer)?;
        zero_platform_tokio::bind_std_datagram_socket_for_peer(peer, interface.as_ref())
    }

    pub async fn bind_tokio(&self, peer: SocketAddr) -> io::Result<TokioDatagramSocket> {
        self.require_direct()?;
        let interface = self.egress.try_current_for_peer(peer)?;
        TokioDatagramSocket::bind_for_peer_on(peer, interface.as_ref()).await
    }

    pub async fn bind_tokio_preserving_port(
        &self,
        peer: SocketAddr,
        preferred_port: Option<u16>,
    ) -> io::Result<TokioDatagramSocket> {
        self.require_direct()?;
        let interface = self.egress.try_current_for_peer(peer)?;
        TokioDatagramSocket::bind_for_peer_on_with_port(peer, interface.as_ref(), preferred_port)
            .await
    }
}

#[derive(Clone)]
struct RelayFactory(crate::datagram_relay::ConnectFn);
impl std::fmt::Debug for RelayFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedDatagramRelay")
    }
}
impl OutboundDatagramSocketFactory {
    pub fn with_relay(mut self, open: crate::datagram_relay::ConnectFn) -> Self {
        self.relay = Some(RelayFactory(open));
        self
    }
    pub fn with_hopping(mut self, profile: Option<crate::datagram_hop::Profile>) -> Self {
        self.hopping = profile;
        self
    }
    pub fn is_relay(&self) -> bool {
        self.relay.is_some()
    }
    pub async fn open_socket(
        &self,
        peer: SocketAddr,
    ) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        if let Some(hopping) = &self.hopping {
            return hopping.open(self.clone(), peer);
        }
        if let Some(relay) = &self.relay {
            return crate::datagram_relay::socket((relay.0)(peer).await?, peer);
        }
        quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, self.bind_std(peer)?)
    }
    fn require_direct(&self) -> io::Result<()> {
        if self.is_relay() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native UDP socket cannot bypass a configured datagram relay",
            ));
        }
        Ok(())
    }
}
