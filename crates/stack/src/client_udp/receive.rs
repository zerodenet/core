//! Authenticated IP reply delivery; correlation and bounded receive queues.
use super::observation::observe_queue_drop;
use super::queue::{enqueue_datagram, QueuedEvent};
use super::{ClientUdpEvent, ClientUdpStack};
use crate::{packet, FragmentOutcome};
use std::borrow::Cow;
use std::time::Instant;
impl ClientUdpStack {
    /// Feed an authenticated, decrypted IP packet. The caller must check that
    /// the packet source belongs to the authenticated tunnel peer first.
    /// Fragment reassembly and socket queues are bounded; excess packets drop.
    pub fn feed(&self, raw_packet: &[u8]) -> bool {
        self.feed_inner(raw_packet, false)
    }

    /// Deliver only replies from the socket's last sent destination. The
    /// caller selects this neutral correlation mode; the stack owns no
    /// endpoint direction policy. A matched packet is consumed even if the
    /// bounded receive queue is full; it must not become new inbound traffic.
    /// ICMP errors already require a matching quote.
    pub fn feed_correlated(&self, raw_packet: &[u8]) -> bool {
        self.feed_inner(raw_packet, true)
    }

    fn feed_inner(&self, raw_packet: &[u8], correlated: bool) -> bool {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.inner.outbound.defer_memory_notifications();
        let packet = {
            let mut fragments = self
                .inner
                .fragments
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match fragments.process(raw_packet, Instant::now()) {
                FragmentOutcome::NotFragmented(packet) => Cow::Borrowed(packet),
                FragmentOutcome::Reassembled(packet) => Cow::Owned(packet),
                FragmentOutcome::Pending | FragmentOutcome::Rejected(_) => return false,
            }
        };
        if let Some(datagram) = packet::parse_udp(&packet) {
            let receiver = {
                let sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
                sockets
                    .get(&datagram.dst)
                    .filter(|socket| !correlated || socket.last_destination == Some(datagram.src))
                    .map(|socket| {
                        (
                            socket.sender.clone(),
                            socket.budget.clone(),
                            socket.observer.clone(),
                        )
                    })
            };
            let Some((sender, budget, observer)) = receiver else {
                return false;
            };
            // Original table selection is complete. No callback, payload copy,
            // channel wake or refund below runs under the socket table mutex.
            if let Some(observer) = &observer {
                observer.received(packet.len());
            }
            #[cfg(feature = "packet-memory")]
            let admitted = if self.inner.outbound.has_received_memory() {
                super::queue::enqueue_packet(
                    &sender,
                    &budget,
                    datagram.src,
                    datagram.payload,
                    &self.inner.outbound,
                )
            } else {
                enqueue_datagram(
                    &sender,
                    &budget,
                    datagram.src,
                    datagram.payload.len(),
                    || datagram.payload.to_vec(),
                )
            };
            #[cfg(not(feature = "packet-memory"))]
            let admitted = enqueue_datagram(
                &sender,
                &budget,
                datagram.src,
                datagram.payload.len(),
                || datagram.payload.to_vec(),
            );
            return match admitted {
                Ok(()) => true,
                Err(error) => {
                    observe_queue_drop(
                        observer.as_ref(),
                        self.inner.drop_observer.as_ref(),
                        &error,
                    );
                    correlated
                }
            };
        }
        let Some(error) = packet::parse_icmp_error(&packet) else {
            return false;
        };
        if error.quoted_protocol != packet::IPPROTO_UDP {
            return false;
        }
        let receiver = {
            let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
            let Some(socket) = sockets.get_mut(&error.quoted_source) else {
                return false;
            };
            if socket.last_destination != Some(error.quoted_destination) {
                return false;
            }
            if let packet::IcmpErrorKind::PacketTooBig { mtu: Some(mtu) } = error.kind {
                let minimum = if error.quoted_destination.ip.is_ipv6() {
                    1_280
                } else {
                    68
                };
                let mtu = (mtu as usize).max(minimum).min(self.inner.mtu);
                socket.path_mtu = Some(socket.path_mtu.unwrap_or(self.inner.mtu).min(mtu));
            }
            (socket.sender.clone(), socket.observer.clone())
        };
        let (sender, observer) = receiver;
        if let Some(observer) = &observer {
            observer.received(packet.len());
        }
        sender
            .try_send(QueuedEvent::control(ClientUdpEvent::IcmpError(error)))
            .inspect_err(|error| {
                observe_queue_drop(observer.as_ref(), self.inner.drop_observer.as_ref(), error)
            })
            .is_ok()
            || correlated
    }
}
