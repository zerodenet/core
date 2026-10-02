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

pub(crate) fn socket_observed(
    carrier: Arc<dyn ConnectedDatagram>,
    peer: SocketAddr,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
    if let Some(observer) = &observer {
        observer.datagram_boundary();
    }
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
                    carrier.send(&packet.bytes).await.inspect_err(|_| {
                        if let Some(observer) = &observer {
                            observer.error();
                            observer.dropped_reason(zero_traits::PacketDropReason::IoFailure);
                        }
                    })?;
                    if let Some(observer) = &observer {
                        observer.sent_datagram(packet.bytes.len());
                    }
                }
                Ok::<_, io::Error>(())
            };
            let receiving = async {
                let mut bytes = vec![0; 65536];
                loop {
                    let length = carrier.receive(&mut bytes).await.inspect_err(|_| {
                        if let Some(observer) = &observer {
                            observer.error();
                        }
                    })?;
                    if let Some(observer) = &observer {
                        observer.received_datagram(length);
                    }
                    if length > bytes.len() {
                        if let Some(observer) = &observer {
                            observer.dropped_reason(zero_traits::PacketDropReason::InvalidPacket);
                        }
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
                        if let Some(observer) = &observer {
                            observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
                        }
                        return Ok::<_, io::Error>(());
                    }
                }
            };
            tokio::select! { result = sending => result, result = receiving => result }
        },
    )
}
