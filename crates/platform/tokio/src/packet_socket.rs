//! Shared datagram carrier handle; concrete framing is supplied by its owner.
use std::{future::poll_fn, io, net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;
use zero_traits::PacketSocketIo;
#[derive(Clone)]
pub enum PacketSocket {
    Udp(Arc<UdpSocket>),
    Carrier(Arc<dyn PacketSocketIo<Error = io::Error>>),
}
impl std::fmt::Debug for PacketSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PacketSocket")
            .field(&self.local_addr())
            .finish()
    }
}
impl From<Arc<UdpSocket>> for PacketSocket {
    fn from(socket: Arc<UdpSocket>) -> Self {
        Self::Udp(socket)
    }
}
impl PacketSocket {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Udp(socket) => socket.local_addr(),
            Self::Carrier(socket) => socket
                .local_addr()
                .map(crate::socket_address_to_socket_addr),
        }
    }
    pub fn udp(&self) -> io::Result<Arc<UdpSocket>> {
        match self {
            Self::Udp(socket) => Ok(socket.clone()),
            _ => Err(io::Error::other("prepared listener requires a UDP socket")),
        }
    }
    pub async fn recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        match self {
            Self::Udp(socket) => socket.recv_from(bytes).await,
            Self::Carrier(socket) => poll_fn(|cx| socket.poll_recv_from(cx, bytes))
                .await
                .map(|(n, peer)| (n, crate::socket_address_to_socket_addr(peer))),
        }
    }
    pub async fn send_to(&self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        match self {
            Self::Udp(socket) => socket.send_to(bytes, peer).await,
            Self::Carrier(socket) => {
                socket
                    .send_to(bytes, crate::socket_addr_to_socket_address(peer))
                    .await
            }
        }
    }
}
