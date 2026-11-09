//! Original payload custody through the same socket queue. Library Vec APIs
//! keep their old explicit conversion; Kernel receivers move this packet.
use super::*;
use std::task::{Context, Poll};
#[derive(Debug)]
pub struct ClientUdpPacketDatagram {
    pub source: Endpoint,
    pub payload: zero_traits::PacketBuffer,
}
#[derive(Debug)]
pub enum ClientUdpPacketEvent {
    Datagram(ClientUdpPacketDatagram),
    IcmpError(packet::ParsedIcmpError),
}
impl ClientUdpPacketEvent {
    pub(super) fn into_library_event(self) -> ClientUdpEvent {
        match self {
            Self::Datagram(packet) => ClientUdpEvent::Datagram(ClientUdpDatagram {
                source: packet.source,
                payload: packet.payload.into_vec(),
            }),
            Self::IcmpError(error) => ClientUdpEvent::IcmpError(error),
        }
    }
}
impl From<ClientUdpEvent> for ClientUdpPacketEvent {
    fn from(event: ClientUdpEvent) -> Self {
        match event {
            ClientUdpEvent::Datagram(packet) => Self::Datagram(ClientUdpPacketDatagram {
                source: packet.source,
                payload: packet.payload.into(),
            }),
            ClientUdpEvent::IcmpError(error) => Self::IcmpError(error),
        }
    }
}
impl ClientUdpSocket {
    /// Move the same queued owner to its actual receiver. Dequeuing ends the
    /// old queue-only byte charge, not the original Root's memory custody.
    pub fn poll_recv_packet_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ClientUdpPacketEvent>> {
        if self.inner.closed.load(Ordering::Acquire) || self.state.closed() {
            self.discard_queued();
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(event) => {
                if self.inner.closed.load(Ordering::Acquire) || self.state.closed() {
                    if event.is_some() {
                        self.observe_closed_discard();
                    }
                    drop(event);
                    self.discard_queued();
                    Poll::Ready(None)
                } else {
                    Poll::Ready(event.map(QueuedEvent::into_packet_event))
                }
            }
        }
    }
}
#[cfg(all(test, feature = "packet-memory"))]
mod tests;

impl ClientUdpStack {
    /// Consume the actual original complete IP reply into its UDP range.
    /// Misses return that same original for the existing Source routing path.
    pub fn feed_packet(
        &self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<(), zero_traits::PacketBuffer> {
        self.feed_received_packet(packet, false)
    }
    pub fn feed_correlated_packet(
        &self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<(), zero_traits::PacketBuffer> {
        self.feed_received_packet(packet, true)
    }
    fn feed_received_packet(
        &self,
        packet: zero_traits::PacketBuffer,
        correlated: bool,
    ) -> Result<(), zero_traits::PacketBuffer> {
        // Keep the old explicit fragment/ICMP semantics until its actual
        // assembly/cache is separately migrated; that existing parser keeps
        // using the same Source-bound queue admission, never another Root.
        if packet::parse_ip_fragment(&packet).is_some()
            || packet::parse_icmp_error(&packet).is_some()
        {
            return if if correlated {
                self.feed_correlated(&packet)
            } else {
                self.feed(&packet)
            } {
                Ok(())
            } else {
                Err(packet)
            };
        }
        let Some(datagram) = packet::parse_udp(&packet) else {
            return Err(packet);
        };
        let source = datagram.src;
        let start = datagram.payload.as_ptr() as usize - packet.as_ptr() as usize;
        let range = start..start + datagram.payload.len();
        let receiver = {
            let sockets = self
                .inner
                .sockets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sockets
                .get(&datagram.dst)
                .filter(|socket| !correlated || socket.last_destination == Some(source))
                .map(|socket| {
                    (
                        socket.sender.clone(),
                        socket.budget.clone(),
                        socket.observer.clone(),
                    )
                })
        };
        let Some((sender, budget, observer)) = receiver else {
            return Err(packet);
        };
        #[cfg(feature = "packet-memory")]
        let _notifications = self.inner.outbound.defer_memory_notifications();
        if let Some(observer) = &observer {
            observer.received(packet.len());
        }
        #[cfg(feature = "packet-memory")]
        if self.inner.outbound.has_received_memory() {
            return match super::queue::enqueue_original_packet(
                &sender,
                &budget,
                source,
                packet,
                range,
                &self.inner.outbound,
            ) {
                Ok(()) => Ok(()),
                Err(error) => {
                    super::observation::observe_queue_drop(
                        observer.as_ref(),
                        self.inner.drop_observer.as_ref(),
                        &error,
                    );
                    // As in feed_correlated, an exact matched reply that is
                    // refused by bounded admission cannot become new traffic.
                    Ok(())
                }
            };
        }
        // Explicit existing unbound/library sockets retain their old Vec API.
        let accepted =
            super::queue::enqueue_datagram(&sender, &budget, source, range.len(), || {
                packet[range].to_vec()
            });
        match accepted {
            Ok(()) => Ok(()),
            Err(error) => {
                super::observation::observe_queue_drop(
                    observer.as_ref(),
                    self.inner.drop_observer.as_ref(),
                    &error,
                );
                if correlated {
                    Ok(())
                } else {
                    Err(packet)
                }
            }
        }
    }
}
