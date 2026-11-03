use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_traits::{PacketSocketIo, SocketAddress};
/// A packet peer delivered by this actual masked original, not an invented UDP
/// native claim. Its permit stays with all stream sender clones until return.
pub struct Peer {
    pub socket: PacketSocket,
    pub remote: SocketAddr,
    pub packets: mpsc::Receiver<Vec<u8>>,
    pub facts: InboundConnectionFacts,
}
impl Peer {
    pub(super) fn new(
        socket: PacketSocket,
        remote: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
        permit: Permit,
        facts: InboundConnectionFacts,
    ) -> Self {
        Self {
            socket: PacketSocket::Carrier(Arc::new(Leased {
                socket,
                _permit: permit,
            })),
            remote,
            packets,
            facts,
        }
    }
}
struct Leased {
    socket: PacketSocket,
    _permit: Permit,
}
impl PacketSocketIo for Leased {
    type Error = io::Error;
    fn local_addr(&self) -> io::Result<SocketAddress> {
        self.socket
            .local_addr()
            .map(zero_platform_tokio::socket_addr_to_socket_address)
    }
    fn poll_recv_from(
        &self,
        _: &mut Context<'_>,
        _: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddress)>> {
        Poll::Ready(Err(io::Error::other("original source owns receive")))
    }
    fn send_to<'a>(
        &'a self,
        bytes: &'a [u8],
        peer: SocketAddress,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            // A retained logical sender cannot keep issuing business I/O after
            // its original generation retires. Custody still ends only on Drop;
            // this stop request is neither a driver join nor native close.
            tokio::select! {biased;
                _=self._permit.users().cancellation().cancelled()=>Err(io::Error::new(io::ErrorKind::NotConnected,"original carrier peer is retired")),
                result=self.socket.send_to(bytes,zero_platform_tokio::socket_address_to_socket_addr(peer))=>result,
            }
        })
    }
}
