use std::{io, sync::Arc};

/// A connected datagram carrier for one Mieru packet underlay.
///
/// The protocol owns packet reliability and session state. Runtime adapters
/// may supply a direct UDP socket or a datagram-capable relay association, but
/// the carrier itself only transports complete encrypted Mieru packets.
#[async_trait::async_trait]
pub trait ClientDatagramCarrier: Send + Sync + 'static {
    async fn send(&self, packet: &[u8]) -> io::Result<()>;

    async fn receive(&self, packet: &mut [u8]) -> io::Result<usize>;
}

pub(crate) struct UdpClientDatagramCarrier {
    socket: Arc<tokio::net::UdpSocket>,
    peer: std::net::SocketAddr,
}

impl UdpClientDatagramCarrier {
    pub(crate) fn new(socket: Arc<tokio::net::UdpSocket>, peer: std::net::SocketAddr) -> Self {
        Self { socket, peer }
    }
}

#[async_trait::async_trait]
impl ClientDatagramCarrier for UdpClientDatagramCarrier {
    async fn send(&self, packet: &[u8]) -> io::Result<()> {
        self.socket.send_to(packet, self.peer).await?;
        Ok(())
    }

    async fn receive(&self, packet: &mut [u8]) -> io::Result<usize> {
        loop {
            let (read, source) = self.socket.recv_from(packet).await?;
            if source == self.peer {
                return Ok(read);
            }
        }
    }
}

/// Runtime-provided socket; observation remains at its actual datagram I/O.
#[cfg(feature = "runtime")]
pub(crate) struct ObservedClientDatagramCarrier {
    socket: zero_platform_tokio::TokioDatagramSocket,
    peer: std::net::SocketAddr,
}
#[cfg(feature = "runtime")]
impl ObservedClientDatagramCarrier {
    pub fn new(
        socket: zero_platform_tokio::TokioDatagramSocket,
        peer: std::net::SocketAddr,
    ) -> Self {
        Self { socket, peer }
    }
}
#[cfg(feature = "runtime")]
#[async_trait::async_trait]
impl ClientDatagramCarrier for ObservedClientDatagramCarrier {
    async fn send(&self, packet: &[u8]) -> std::io::Result<()> {
        self.socket
            .send_to_addr(packet, self.peer)
            .await
            .map(|_| ())
    }
    async fn receive(&self, packet: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let (size, source) = self.socket.recv_from_addr(packet).await?;
            if source == self.peer {
                return Ok(size);
            }
        }
    }
}
