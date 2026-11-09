//! User-space UDP stack.
//!
//! Implements [`UdpStack`] by extracting UDP datagrams from raw IP
//! packets and forwarding them through internal queues.  Outbound
//! datagrams are wrapped in IP + UDP headers and emitted through
//! the outbound packet channel.

#[cfg(feature = "packet-memory")]
mod feedback;
#[cfg(feature = "packet-memory")]
mod owned;
mod queue;
mod quiescence;
mod transmit;
use queue::Queue;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, Notify};
use tracing::warn;

use zero_traits::{SocketAddress, UdpStack};

use crate::packet::{self};

// ── Queued datagram ───────────────────────────────────────────────────

#[derive(Debug)]
pub struct UdpStackDatagram {
    pub peer_identity: Option<Arc<str>>,
    pub data: zero_traits::PacketBuffer,
    pub source: SocketAddress,
    pub destination: SocketAddress,
}

fn endpoint_to_sockaddr(ep: &crate::packet::Endpoint) -> SocketAddress {
    use std::net::IpAddr;
    let ip = match ep.ip {
        IpAddr::V4(v4) => zero_traits::IpAddress::V4(v4.octets()),
        IpAddr::V6(v6) => zero_traits::IpAddress::V6(v6.octets()),
    };
    SocketAddress::new(ip, ep.port)
}

fn sockaddr_to_ipaddr(sa: &SocketAddress) -> std::net::IpAddr {
    match sa.ip {
        zero_traits::IpAddress::V4(octets) => std::net::IpAddr::V4(octets.into()),
        zero_traits::IpAddress::V6(octets) => std::net::IpAddr::V6(octets.into()),
    }
}

// ── UserUdpStack ──────────────────────────────────────────────────────

/// User-space UDP forwarding stack.
///
/// Implements [`UdpStack`].  Inbound UDP packets are queued via
/// [`feed`] and consumed via [`recv_from`].  Outbound datagrams
/// sent via [`send_to`] are wrapped in IP + UDP headers and pushed
/// through the outbound channel.
///
/// [`feed`]: UdpStack::feed
/// [`recv_from`]: UdpStack::recv_from
/// [`send_to`]: UdpStack::send_to
pub struct UserUdpStack {
    datagrams: Mutex<Queue>,
    available: Notify,
    outbound: crate::packet_output::PacketSender,
    mtu: usize,
    next_fragment_id: AtomicU32,
}

impl UserUdpStack {
    pub(crate) fn new(outbound: mpsc::Sender<Vec<u8>>, mtu: usize) -> Self {
        Self::new_output(outbound.into(), mtu)
    }
    pub(crate) fn new_output(outbound: crate::packet_output::PacketSender, mtu: usize) -> Self {
        Self {
            datagrams: Mutex::new(Queue::new(&outbound)),
            available: Notify::new(),
            outbound,
            mtu,
            next_fragment_id: AtomicU32::new(1),
        }
    }

    /// Best-effort, non-blocking feedback for a UDP datagram rejected before
    /// an association can accept it.
    pub fn send_unreachable(
        &self,
        payload: &[u8],
        source: SocketAddress,
        destination: SocketAddress,
    ) -> bool {
        #[cfg(feature = "packet-memory")]
        if self.outbound.has_transmit_allocator() {
            return self.send_unreachable_owned(payload, source, destination);
        }
        let original = packet::build_udp(
            sockaddr_to_ipaddr(&source),
            sockaddr_to_ipaddr(&destination),
            source.port,
            destination.port,
            payload,
        );
        let Some(response) = packet::build_udp_unreachable_response(&original, self.mtu) else {
            return false;
        };
        self.outbound.try_send(response).is_ok()
    }
}

impl UserUdpStack {
    /// Feed IP input with opaque authenticated peer provenance.
    pub async fn feed_with_peer(&self, packet: &[u8], peer_identity: Option<Arc<str>>) {
        if packet::ip_protocol(packet) != Some(packet::IPPROTO_UDP) {
            return;
        }
        let udp = match packet::parse_udp(packet) {
            Some(u) => u,
            None => return,
        };
        if udp.payload.is_empty() {
            return;
        }

        #[cfg(feature = "packet-memory")]
        let memory = if self.outbound.has_received_memory() {
            let Some(view) = self.outbound.packet_memory_view() else {
                warn!("actual UDP Source memory is not installed");
                return;
            };
            Some(view)
        } else {
            None
        };
        // The synchronous guard precedes the short queue lock: all queued
        // drops/refunds unlock before any external Root notification runs.
        #[cfg(feature = "packet-memory")]
        let _notifications = memory.as_ref().map(|view| view.defer_notifications());
        let mut dgrams = self
            .datagrams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if dgrams.len() >= 1024 {
            warn!("udp datagram queue full, dropping");
            return;
        }
        #[cfg(feature = "packet-memory")]
        let data = match memory.as_ref() {
            Some(_) => match self.outbound.copy_received_payload(udp.payload) {
                Ok(packet) => packet,
                Err(source) => {
                    warn!(?source, "UDP receive Root capacity rejected new datagram");
                    return;
                }
            },
            None => udp.payload.to_vec().into(), // Existing explicit library output.
        };
        #[cfg(not(feature = "packet-memory"))]
        let data = udp.payload.to_vec().into();
        let datagram = UdpStackDatagram {
            peer_identity,
            data,
            source: endpoint_to_sockaddr(&udp.src),
            destination: endpoint_to_sockaddr(&udp.dst),
        };
        if dgrams
            .push(
                datagram,
                #[cfg(feature = "packet-memory")]
                memory.as_ref(),
            )
            .is_err()
        {
            warn!("UDP receive Root metadata rejected new queue row");
            return;
        }
        drop(dgrams);
        self.available.notify_one();
    }

    fn try_recv_datagram(&self) -> Option<UdpStackDatagram> {
        // Even the last row/table and payload drop happen outside this lock.
        let (datagram, empty_table) = {
            self.datagrams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop()
        };
        drop(empty_table);
        datagram
    }
    pub async fn recv_datagram_with_peer(&self) -> Option<UdpStackDatagram> {
        loop {
            let notified = self.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(datagram) = self.try_recv_datagram() {
                return Some(datagram);
            }
            notified.await;
        }
    }
    /// Receive a datagram and its original device-supplied provenance.
    pub async fn recv_from_with_peer(
        &self,
        buf: &mut [u8],
    ) -> Option<(usize, SocketAddress, SocketAddress, Option<Arc<str>>)> {
        loop {
            let notified = self.available.notified();
            if let Some(dgram) = self.try_recv_datagram() {
                let n = dgram.data.len().min(buf.len());
                buf[..n].copy_from_slice(&dgram.data[..n]);
                return Some((n, dgram.source, dgram.destination, dgram.peer_identity));
            }
            notified.await;
        }
    }
}

impl UdpStack for UserUdpStack {
    async fn feed(&self, packet: &[u8]) {
        self.feed_with_peer(packet, None).await;
    }
    async fn recv_from(&self, buf: &mut [u8]) -> Option<(usize, SocketAddress, SocketAddress)> {
        self.recv_from_with_peer(buf)
            .await
            .map(|(n, src, dst, _)| (n, src, dst))
    }
    async fn send_to(&self, data: &[u8], src: SocketAddress, dst: SocketAddress) {
        self.send_datagram(data, src, dst).await;
    }
}

#[cfg(all(test, feature = "packet-memory"))]
mod tests;
