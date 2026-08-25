//! User-space UDP stack.
//!
//! Implements [`UdpStack`] by extracting UDP datagrams from raw IP
//! packets and forwarding them through internal queues.  Outbound
//! datagrams are wrapped in IP + UDP headers and emitted through
//! the outbound packet channel.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::{mpsc, Mutex, Notify};
use tracing::warn;

use zero_traits::{SocketAddress, UdpStack};

use crate::packet::{self};

// ── Queued datagram ───────────────────────────────────────────────────

struct Datagram {
    data: Vec<u8>,
    src: SocketAddress,
    dst: SocketAddress,
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
    datagrams: Mutex<VecDeque<Datagram>>,
    available: Notify,
    outbound: mpsc::Sender<Vec<u8>>,
    mtu: usize,
    next_fragment_id: AtomicU32,
}

impl UserUdpStack {
    pub(crate) fn new(outbound: mpsc::Sender<Vec<u8>>, mtu: usize) -> Self {
        Self {
            datagrams: Mutex::new(VecDeque::new()),
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

impl UdpStack for UserUdpStack {
    async fn feed(&self, packet: &[u8]) {
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

        let mut dgrams = self.datagrams.lock().await;
        // Bound the queue to avoid unbounded growth.
        if dgrams.len() >= 1024 {
            warn!("udp datagram queue full, dropping");
            return;
        }
        dgrams.push_back(Datagram {
            data: udp.payload.to_vec(),
            src: endpoint_to_sockaddr(&udp.src),
            dst: endpoint_to_sockaddr(&udp.dst),
        });
        drop(dgrams);
        self.available.notify_one();
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Option<(usize, SocketAddress, SocketAddress)> {
        loop {
            let notified = self.available.notified();
            if let Some(dgram) = self.datagrams.lock().await.pop_front() {
                let n = dgram.data.len().min(buf.len());
                buf[..n].copy_from_slice(&dgram.data[..n]);
                return Some((n, dgram.src, dgram.dst));
            }
            notified.await;
        }
    }

    async fn send_to(&self, data: &[u8], src: SocketAddress, dst: SocketAddress) {
        let src_ip = sockaddr_to_ipaddr(&src);
        let dst_ip = sockaddr_to_ipaddr(&dst);
        let maximum_payload = if src_ip.is_ipv4() { 65_507 } else { 65_527 };
        if data.len() > maximum_payload {
            warn!(
                payload_bytes = data.len(),
                maximum_payload, "UDP payload exceeds the IP datagram limit"
            );
            return;
        }
        let pkt = packet::build_udp(src_ip, dst_ip, src.port, dst.port, data);
        let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        let fragments = packet::fragment_ip_packet(&pkt, self.mtu, identification);
        if fragments.is_empty() {
            warn!(
                packet_bytes = pkt.len(),
                mtu = self.mtu,
                "unable to fragment UDP packet"
            );
            return;
        }
        for fragment in fragments {
            if self.outbound.send(fragment).await.is_err() {
                warn!("UDP outbound packet transport closed");
                return;
            }
        }
    }
}
