use std::io;
use std::net::SocketAddr;

use zero_platform_tokio::{EgressInterfaceControl, TokioDatagramSocket};

/// Runtime-owned factory for every external UDP/QUIC socket.
///
/// The control handle is read when a socket is created, so route
/// reconciliation affects new flows without mutating established sockets.
#[derive(Clone, Debug)]
pub struct OutboundDatagramSocketFactory {
    egress: EgressInterfaceControl,
}

impl OutboundDatagramSocketFactory {
    pub fn new(egress: EgressInterfaceControl) -> Self {
        Self { egress }
    }

    pub fn egress_for(&self, peer: SocketAddr) -> Option<zero_platform_tokio::EgressInterface> {
        self.egress.current_for_peer(peer)
    }

    pub fn bind_std(&self, peer: SocketAddr) -> io::Result<std::net::UdpSocket> {
        let interface = self.egress.try_current_for_peer(peer)?;
        zero_platform_tokio::bind_std_datagram_socket_for_peer(peer, interface.as_ref())
    }

    pub async fn bind_tokio(&self, peer: SocketAddr) -> io::Result<TokioDatagramSocket> {
        let interface = self.egress.try_current_for_peer(peer)?;
        TokioDatagramSocket::bind_for_peer_on(peer, interface.as_ref()).await
    }
}
