//! A connected packet carrier supplied by a prepared relay prefix.
use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};

#[async_trait::async_trait]
pub trait ConnectedDatagram: Send + Sync {
    async fn send(&self, packet: &[u8]) -> io::Result<()>;
    async fn receive(&self, packet: &mut [u8]) -> io::Result<usize>;
}
pub type ConnectFuture =
    Pin<Box<dyn Future<Output = io::Result<Arc<dyn ConnectedDatagram>>> + Send>>;
pub type ConnectFn = Arc<dyn Fn(SocketAddr) -> ConnectFuture + Send + Sync>;

pub(crate) fn socket(
    carrier: Arc<dyn ConnectedDatagram>,
    peer: SocketAddr,
) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
    let local = SocketAddr::new(
        if peer.is_ipv4() {
            std::net::Ipv4Addr::UNSPECIFIED.into()
        } else {
            std::net::Ipv6Addr::UNSPECIFIED.into()
        },
        0,
    );
    crate::datagram_queue::spawn_with(
        local,
        Arc::new(move |packet| {
            if packet.destination != peer {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "connected relay packet changed destination",
                ));
            }
            Ok(packet.contents.to_vec())
        }),
        move |mut channels| async move {
            let sending = async {
                while let Some(packet) = channels.outgoing.recv().await {
                    carrier.send(&packet.bytes).await?;
                }
                Ok::<_, io::Error>(())
            };
            let receiving = async {
                let mut bytes = vec![0; 65536];
                loop {
                    let length = carrier.receive(&mut bytes).await?;
                    if length > bytes.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "oversized relay packet",
                        ));
                    }
                    if channels
                        .incoming
                        .send(crate::datagram_queue::Packet {
                            bytes: bytes[..length].to_vec(),
                            peer,
                        })
                        .await
                        .is_err()
                    {
                        return Ok::<_, io::Error>(());
                    }
                }
            };
            tokio::select! { result = sending => result, result = receiving => result }
        },
    )
}
