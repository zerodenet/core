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
