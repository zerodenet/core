//! Active client-side TCP over a bounded raw-IP packet channel.

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::sync::{oneshot, Mutex};

use super::{
    accept_client_segment, default_peer_mss, key_from_parsed, key_reversed, next_iss,
    run_retransmission_worker, Conn, ConnKey, TcpControlPackets, TcpReceiveBuffer, TcpSendControl,
    TcpState, TcpWrite, UserTcpStream, MAX_TCP_CONNECTIONS, MAX_TCP_HALF_OPEN_CONNECTIONS,
    NEXT_CONNECTION_ID, TCP_RECEIVE_BUFFER_BYTES,
};
use crate::packet::{self, tcp_flags};

const FIRST_EPHEMERAL_PORT: u16 = 49_152;
const EPHEMERAL_PORT_COUNT: usize = 16_384;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTcpStackError {
    MissingLocalAddress,
    InvalidMtu,
    UnknownLocalAddress,
    AddressFamilyMismatch,
    ConnectionLimit,
    OutboundClosed,
    ConnectTimeout,
    ConnectionReset,
    DestinationUnreachable,
}

pub struct ClientTcpStack {
    local_addresses: Vec<IpAddr>,
    connections: Arc<Mutex<HashMap<ConnKey, Conn>>>,
    outbound: crate::packet_output::PacketSender,
    control_packets: TcpControlPackets,
    mss: u16,
    mtu: usize,
    next_fragment_id: AtomicU32,
}

impl ClientTcpStack {
    pub fn new(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<Vec<u8>>,
        mtu: u16,
    ) -> Result<Self, ClientTcpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu)
    }
    pub fn new_observed(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
    ) -> Result<Self, ClientTcpStackError> {
        Self::new_with_output(local_addresses, outbound.into(), mtu)
    }
    fn new_with_output(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
    ) -> Result<Self, ClientTcpStackError> {
        if local_addresses.is_empty() {
            return Err(ClientTcpStackError::MissingLocalAddress);
        }
        if mtu < 68 || (local_addresses.iter().any(IpAddr::is_ipv6) && mtu < 1_280) {
            return Err(ClientTcpStackError::InvalidMtu);
        }
        let mss = crate::tcp_mss_for_mtu(mtu);
        Ok(Self {
            local_addresses,
            connections: Arc::new(Mutex::new(HashMap::new())),
            control_packets: TcpControlPackets::new(outbound.clone()),
            outbound,
            mss,
            mtu: usize::from(mtu),
            next_fragment_id: AtomicU32::new(1),
        })
    }

    pub async fn connect(
        &self,
        local_ip: IpAddr,
        remote: SocketAddr,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        self.connect_observed(local_ip, remote, None).await
    }

    pub async fn connect_observed(
        &self,
        local_ip: IpAddr,
        remote: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        if !self.local_addresses.contains(&local_ip) {
            return Err(ClientTcpStackError::UnknownLocalAddress);
        }
        if local_ip.is_ipv4() != remote.ip().is_ipv4() {
            return Err(ClientTcpStackError::AddressFamilyMismatch);
        }
        self.control_packets.ensure_worker();
        let mut connections = self.connections.lock().await;
        let half_open = connections
            .values()
            .filter(|connection| connection.connect_waiter.is_some())
            .count();
        if connections.len() >= MAX_TCP_CONNECTIONS || half_open >= MAX_TCP_HALF_OPEN_CONNECTIONS {
            return Err(ClientTcpStackError::ConnectionLimit);
        }
        let start = usize::from(rand::random_range(0..EPHEMERAL_PORT_COUNT as u16));
        let Some((key, local_port)) = (0..EPHEMERAL_PORT_COUNT).find_map(|offset| {
            let port = FIRST_EPHEMERAL_PORT + ((start + offset) % EPHEMERAL_PORT_COUNT) as u16;
            let key = (remote.ip(), remote.port(), local_ip, port);
            (!connections.contains_key(&key)).then_some((key, port))
        }) else {
            return Err(ClientTcpStackError::ConnectionLimit);
        };
        let iss = next_iss();
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let (waiter, ready) = oneshot::channel();
        let receive_buffer = Arc::new(TcpReceiveBuffer::new(TCP_RECEIVE_BUFFER_BYTES));
        let send_control = Arc::new(TcpSendControl::new_network(iss, 0, self.mss));
        let syn = packet::build_tcp_with_mss_and_window(
            local_ip,
            remote.ip(),
            local_port,
            remote.port(),
            iss,
            0,
            tcp_flags::SYN,
            self.mss,
            receive_buffer.window(),
        );
        connections.insert(
            key,
            Conn {
                id: connection_id,
                state: TcpState::SynSent,
                snd_nxt: Arc::new(AtomicU32::new(iss.wrapping_add(1))),
                send_control: send_control.clone(),
                rcv_nxt: Arc::new(AtomicU32::new(0)),
                receive_buffer,
                last_active: Instant::now(),
                fin_sent: Arc::new(AtomicBool::new(false)),
                peer_mss: default_peer_mss(remote.ip()),
                path_mtu: None,
                connect_waiter: Some(waiter),
                peer_identity: None,
                observer: observer.clone(),
            },
        );
        drop(connections);
        send_control.track_segment(iss.wrapping_add(1), syn.clone());
        tokio::spawn(run_retransmission_worker(
            Arc::downgrade(&self.connections),
            key,
            connection_id,
            send_control.clone(),
            self.outbound
                .clone()
                .with_observer(observer.clone())
                .downgrade(),
        ));
        if !self
            .control_packets
            .clone()
            .with_observer(observer)
            .try_send(syn)
        {
            self.connections.lock().await.remove(&key);
            return Err(ClientTcpStackError::OutboundClosed);
        }
        match tokio::time::timeout(CONNECT_TIMEOUT, ready).await {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(_)) => match send_control.io_error().map(|error| error.kind()) {
                Some(std::io::ErrorKind::TimedOut) => Err(ClientTcpStackError::ConnectTimeout),
                Some(std::io::ErrorKind::BrokenPipe) => Err(ClientTcpStackError::OutboundClosed),
                Some(std::io::ErrorKind::NetworkUnreachable) => {
                    Err(ClientTcpStackError::DestinationUnreachable)
                }
                _ => Err(ClientTcpStackError::ConnectionReset),
            },
            Err(_) => {
                self.connections.lock().await.remove(&key);
                Err(ClientTcpStackError::ConnectTimeout)
            }
        }
    }

    pub async fn feed(&self, raw_packet: &[u8]) {
        self.control_packets.ensure_worker();
        let Some(tcp) = packet::parse_tcp(raw_packet) else {
            return;
        };
        let key = key_from_parsed(&tcp);
        let rev = key_reversed(&key);
        let mut connections = self.connections.lock().await;
        let Some(conn) = connections.get_mut(&key) else {
            return;
        };
        if let Some(observer) = &conn.observer {
            observer.received(raw_packet.len());
        }
        conn.last_active = Instant::now();
        if tcp.rst {
            conn.send_control.observe_reset();
            connections.remove(&key);
            return;
        }
        let peer_window = packet::tcp_window(raw_packet).unwrap_or(0);
        match conn.state {
            TcpState::SynSent => {
                let expected_ack = conn.snd_nxt.load(Ordering::Acquire);
                if !tcp.syn || !tcp.ack_flag || tcp.ack != expected_ack {
                    return;
                }
                conn.rcv_nxt
                    .store(tcp.seq.wrapping_add(1), Ordering::Release);
                conn.peer_mss = packet::tcp_mss(raw_packet)
                    .filter(|mss| *mss > 0)
                    .unwrap_or_else(|| default_peer_mss(tcp.src.ip));
                conn.send_control
                    .observe_ack(tcp.ack, peer_window, expected_ack);
                conn.state = TcpState::Established;
                let write = TcpWrite::new(
                    self.outbound.clone(),
                    self.control_packets.clone(),
                    &key,
                    conn,
                    conn.peer_mss.min(self.mss),
                );
                let stream = UserTcpStream::new(Arc::clone(&conn.receive_buffer), write);
                let ack = packet::build_tcp_with_window(
                    rev.0,
                    rev.2,
                    rev.1,
                    rev.3,
                    expected_ack,
                    conn.rcv_nxt.load(Ordering::Acquire),
                    tcp_flags::ACK,
                    conn.receive_buffer.window(),
                    &[],
                );
                self.control_packets
                    .clone()
                    .with_observer(conn.observer.clone())
                    .try_send(ack);
                if let Some(waiter) = conn.connect_waiter.take() {
                    let _ = waiter.send(stream);
                }
            }
            TcpState::Established | TcpState::CloseWait => {
                if tcp.ack_flag {
                    conn.send_control.observe_ack(
                        tcp.ack,
                        peer_window,
                        conn.snd_nxt.load(Ordering::Acquire),
                    );
                }
                if tcp.syn {
                    self.send_ack(conn, rev);
                    return;
                }
                let (needs_ack, accepted_fin) = accept_client_segment(conn, &tcp);
                if needs_ack {
                    self.send_ack(conn, rev);
                }
                if accepted_fin
                    && conn.fin_sent.load(Ordering::Acquire)
                    && tcp.ack_flag
                    && tcp.ack == conn.snd_nxt.load(Ordering::Acquire)
                {
                    conn.send_control.stop();
                    connections.remove(&key);
                }
            }
            TcpState::SynReceived => {}
        }
    }

    /// Whether an authenticated packet belongs to an active client flow.
    /// A shared endpoint uses this to distinguish replies from new ingress.
    pub async fn has_connection(&self, raw_packet: &[u8]) -> bool {
        let Some(tcp) = packet::parse_tcp(raw_packet) else {
            return false;
        };
        self.connections
            .lock()
            .await
            .contains_key(&key_from_parsed(&tcp))
    }

    /// Apply Packet Too Big to the matching client connection. Hard errors
    /// fail only a matching half-open connection.
    pub async fn feed_icmp_error(&self, error: packet::ParsedIcmpError) -> bool {
        self.feed_icmp_error_observed(error, None).await
    }
    pub async fn feed_icmp_error_observed(
        &self,
        error: packet::ParsedIcmpError,
        bytes: Option<usize>,
    ) -> bool {
        if error.quoted_protocol != packet::IPPROTO_TCP {
            return false;
        }
        let key = (
            error.quoted_destination.ip,
            error.quoted_destination.port,
            error.quoted_source.ip,
            error.quoted_source.port,
        );
        let mut connections = self.connections.lock().await;
        let observed_match = bytes.is_some() && connections.contains_key(&key);
        if let Some(connection) = connections.get(&key) {
            if let (Some(observer), Some(bytes)) = (&connection.observer, bytes) {
                observer.received(bytes);
            }
        }
        if let packet::IcmpErrorKind::PacketTooBig { mtu: Some(mtu) } = error.kind {
            let Some(connection) = connections.get_mut(&key) else {
                return false;
            };
            let minimum = if error.quoted_destination.ip.is_ipv6() {
                1_280
            } else {
                68
            };
            let mtu = (mtu as usize).max(minimum).min(self.mtu);
            let path_mtu = connection.path_mtu.unwrap_or(self.mtu).min(mtu);
            connection.path_mtu = Some(path_mtu);
            connection
                .send_control
                .reduce_path_mss(crate::tcp_mss_for_mtu(path_mtu as u16));
            return true;
        }
        if !matches!(
            error.kind,
            packet::IcmpErrorKind::DestinationUnreachable { .. }
                | packet::IcmpErrorKind::ParameterProblem { .. }
        ) {
            return observed_match;
        }
        if !connections
            .get(&key)
            .is_some_and(|connection| connection.state == TcpState::SynSent)
        {
            return observed_match;
        }
        let connection = connections.remove(&key).expect("checked connection exists");
        connection.send_control.observe_unreachable();
        true
    }

    /// Split an outbound TCP packet at the learned path MTU. This also covers
    /// retransmitted packets, which were built before Packet Too Big arrived.
    pub async fn fragment_outbound_packet(&self, raw_packet: &[u8]) -> Vec<Vec<u8>> {
        let Some(mtu) = self.outbound_packet_mtu(raw_packet).await else {
            return Vec::new();
        };
        let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        packet::fragment_ip_packet(raw_packet, mtu, identification)
    }

    /// Use the same learned path MTU while retaining an unfragmented buffer.
    pub async fn fragment_outbound_packet_owned(&self, raw_packet: Vec<u8>) -> Vec<Vec<u8>> {
        let Some(mtu) = self.outbound_packet_mtu(&raw_packet).await else {
            return Vec::new();
        };
        let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        packet::fragment_ip_packet_owned(raw_packet, mtu, identification)
    }

    async fn outbound_packet_mtu(&self, raw_packet: &[u8]) -> Option<usize> {
        let tcp = packet::parse_tcp(raw_packet)?;
        let key = (tcp.dst.ip, tcp.dst.port, tcp.src.ip, tcp.src.port);
        Some(
            self.connections
                .lock()
                .await
                .get(&key)
                .and_then(|connection| connection.path_mtu)
                .unwrap_or(self.mtu),
        )
    }

    fn send_ack(&self, conn: &Conn, rev: ConnKey) {
        self.control_packets
            .clone()
            .with_observer(conn.observer.clone())
            .try_send(packet::build_tcp_with_window(
                rev.0,
                rev.2,
                rev.1,
                rev.3,
                conn.snd_nxt.load(Ordering::Acquire),
                conn.rcv_nxt.load(Ordering::Acquire),
                tcp_flags::ACK,
                conn.receive_buffer.window(),
                &[],
            ));
    }

    pub async fn cleanup_idle(&self, timeout: Duration) {
        let mut connections = self.connections.lock().await;
        connections.retain(|_, conn| {
            let keep = conn.last_active.elapsed() < timeout;
            if !keep {
                conn.send_control.expire();
            }
            keep
        });
    }
}
