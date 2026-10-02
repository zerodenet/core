//! Authenticated IP reply delivery; correlation and bounded receive queues.
use super::observation::observe_queue_drop;
use super::{ClientUdpDatagram, ClientUdpEvent, ClientUdpStack};
use crate::{packet, FragmentOutcome};
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
        let packet = {
            let mut fragments = self
                .inner
                .fragments
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match fragments.process(raw_packet, Instant::now()) {
                FragmentOutcome::NotFragmented(packet) => packet.to_vec(),
                FragmentOutcome::Reassembled(packet) => packet,
                FragmentOutcome::Pending | FragmentOutcome::Rejected(_) => return false,
            }
        };
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(datagram) = packet::parse_udp(&packet) {
            return sockets.get(&datagram.dst).is_some_and(|socket| {
                if correlated && socket.last_destination != Some(datagram.src) {
                    return false;
                }
                if let Some(observer) = &socket.observer {
                    observer.received(packet.len());
                }
                socket
                    .sender
                    .try_send(ClientUdpEvent::Datagram(ClientUdpDatagram {
                        source: datagram.src,
                        payload: datagram.payload.to_vec(),
                    }))
                    .inspect_err(|error| {
                        observe_queue_drop(
                            socket.observer.as_ref(),
                            self.inner.drop_observer.as_ref(),
                            error,
                        );
                    })
                    .is_ok()
                    || correlated
            });
        }
        let Some(error) = packet::parse_icmp_error(&packet) else {
            return false;
        };
        if error.quoted_protocol != packet::IPPROTO_UDP {
            return false;
        }
        sockets.get_mut(&error.quoted_source).is_some_and(|socket| {
            if socket.last_destination != Some(error.quoted_destination) {
                return false;
            }
            if let Some(observer) = &socket.observer {
                observer.received(packet.len());
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
            socket
                .sender
                .try_send(ClientUdpEvent::IcmpError(error))
                .inspect_err(|error| {
                    observe_queue_drop(
                        socket.observer.as_ref(),
                        self.inner.drop_observer.as_ref(),
                        error,
                    );
                })
                .is_ok()
                || correlated
        })
    }
}
