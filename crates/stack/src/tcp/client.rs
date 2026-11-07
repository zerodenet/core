//! Active client-side TCP over a bounded raw-IP packet channel.

mod quiescence;
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

mod control_receive;
mod opening;
mod receive;
pub use control_receive::ClientTcpControlReceive;
pub use opening::{ClientTcpClosed, ClientTcpOpening, ClientTcpSocketControl};

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
    PacketMemory(zero_traits::PacketDeriveError),
    /// Actual receive backing could not be admitted before any SYN submission.
    ReceiveMemory(zero_traits::PacketDeriveError),
}

impl std::fmt::Display for ClientTcpStackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client TCP stack: {self:?}")
    }
}
impl std::error::Error for ClientTcpStackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PacketMemory(source) | Self::ReceiveMemory(source) => Some(source),
            _ => None,
        }
    }
}

pub struct ClientTcpStack {
    local_addresses: Vec<IpAddr>,
    connections: Arc<Mutex<HashMap<ConnKey, Conn>>>,
    outbound: crate::packet_output::PacketSender,
    control_packets: TcpControlPackets,
    mss: u16,
    mtu: usize,
    next_fragment_id: AtomicU32,
    revoked: AtomicBool,
    task_spawner: crate::task_owner::TcpTaskSpawner,
}

impl ClientTcpStack {
    /// The original Source/Device driver observes this same retained cause.
    pub fn output_failure(&self) -> Option<Arc<zero_traits::PacketDeriveError>> {
        self.control_packets.failure()
    }

    /// Actual active connection ceiling; retiring transferred users may still
    /// occupy their owning host's admission budget after this table drains.
    pub const CONNECTION_LIMIT: usize = MAX_TCP_CONNECTIONS;

    /// One original retransmission worker per connection and one control worker.
    pub const BACKGROUND_TASK_LIMIT: usize = MAX_TCP_CONNECTIONS + 1;

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
        Self::new_with_output_and_tasks(
            local_addresses,
            outbound,
            mtu,
            crate::task_owner::TcpTaskSpawner::default(),
        )
    }

    /// Register the original client workers with the supplied persistent owner.
    /// Construction is cold; failed admission never falls back to Tokio spawn.
    pub fn new_observed_with_task_owner(
        local_addresses: Vec<IpAddr>,
        outbound: tokio::sync::mpsc::Sender<crate::packet_output::ObservedPacket>,
        mtu: u16,
        owner: Arc<dyn crate::TcpTaskOwner>,
    ) -> Result<Self, ClientTcpStackError> {
        Self::new_with_output_and_tasks(
            local_addresses,
            outbound.into(),
            mtu,
            crate::task_owner::TcpTaskSpawner::owned(owner),
        )
    }

    pub fn new_output_with_task_owner(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
        owner: Arc<dyn crate::TcpTaskOwner>,
    ) -> Result<Self, ClientTcpStackError> {
        Self::new_with_output_and_tasks(
            local_addresses,
            outbound,
            mtu,
            crate::task_owner::TcpTaskSpawner::owned(owner),
        )
    }
    fn new_with_output_and_tasks(
        local_addresses: Vec<IpAddr>,
        outbound: crate::packet_output::PacketSender,
        mtu: u16,
        task_spawner: crate::task_owner::TcpTaskSpawner,
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
            control_packets: TcpControlPackets::new_with_tasks(
                outbound.clone(),
                task_spawner.clone(),
            ),
            outbound,
            mss,
            mtu: usize::from(mtu),
            next_fragment_id: AtomicU32::new(1),
            revoked: AtomicBool::new(false),
            task_spawner,
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
        self.connect_original(local_ip, remote, observer, None)
            .await
    }
    async fn connect_original(
        &self,
        local_ip: IpAddr,
        remote: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        original: Option<&ClientTcpSocketControl>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        if original.is_some_and(ClientTcpSocketControl::is_stopped) {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        if !self.local_addresses.contains(&local_ip) {
            return Err(ClientTcpStackError::UnknownLocalAddress);
        }
        if local_ip.is_ipv4() != remote.ip().is_ipv4() {
            return Err(ClientTcpStackError::AddressFamilyMismatch);
        }
        if self.revoked.load(Ordering::Acquire) || !self.control_packets.ensure_worker() {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        let (key, connection_id, send_control, syn, ready, iss) = {
            let locked = self.connections.lock().await;
            #[cfg(feature = "packet-memory")]
            let _memory_notifications = self.outbound.defer_memory_notifications();
            let mut connections = locked;
            if self.revoked.load(Ordering::Acquire) {
                return Err(ClientTcpStackError::OutboundClosed);
            }
            let half_open = connections
                .values()
                .filter(|connection| connection.connect_waiter.is_some())
                .count();
            if connections.len() >= MAX_TCP_CONNECTIONS
                || half_open >= MAX_TCP_HALF_OPEN_CONNECTIONS
            {
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
            let receive_buffer = Arc::new(
                TcpReceiveBuffer::new_for_output(TCP_RECEIVE_BUFFER_BYTES, &self.outbound)
                    .map_err(ClientTcpStackError::ReceiveMemory)?,
            );
            let send_control = Arc::new(TcpSendControl::new_network(iss, 0, self.mss));
            let syn = super::output::CachedPacket::build(
                packet::TcpPacketSpec {
                    source: local_ip,
                    destination: remote.ip(),
                    source_port: local_port,
                    destination_port: remote.port(),
                    sequence: iss,
                    acknowledgment: 0,
                    flags: tcp_flags::SYN,
                    window: receive_buffer.window(),
                    mss: Some(self.mss),
                    payload: &[],
                },
                &self.outbound,
            )
            .map_err(ClientTcpStackError::PacketMemory)?;
            if let Some(original) = original {
                original.install(key, connection_id, send_control.clone())?;
            }
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
            (key, connection_id, send_control, syn, ready, iss)
        };
        if original.is_some_and(ClientTcpSocketControl::is_stopped) {
            drop(self.remove_connection(&key).await);
            return Err(ClientTcpStackError::OutboundClosed);
        }
        send_control.track_segment(iss.wrapping_add(1), syn.clone());
        if !self.task_spawner.spawn(run_retransmission_worker(
            Arc::downgrade(&self.connections),
            key,
            connection_id,
            send_control.clone(),
            self.outbound
                .clone()
                .with_observer(observer.clone())
                .downgrade(),
        )) {
            send_control.expire();
            drop(self.remove_connection(&key).await);
            return Err(ClientTcpStackError::OutboundClosed);
        }
        if !self
            .control_packets
            .clone()
            .with_observer(observer)
            .try_send_cached(&syn)
        {
            drop(self.remove_connection(&key).await);
            return Err(match self.control_packets.failure() {
                Some(source) => ClientTcpStackError::PacketMemory(*source),
                None => ClientTcpStackError::OutboundClosed,
            });
        }
        let ready = async {
            match original {
                Some(original) => tokio::select! {
                    biased;
                    _ = original.cancelled() => None,
                    result = ready => result.ok(),
                },
                None => ready.await.ok(),
            }
        };
        match tokio::time::timeout(CONNECT_TIMEOUT, ready).await {
            Ok(Some(stream)) => Ok(stream),
            Ok(None) => match send_control.io_error().map(|error| error.kind()) {
                Some(std::io::ErrorKind::TimedOut) => Err(ClientTcpStackError::ConnectTimeout),
                Some(std::io::ErrorKind::BrokenPipe) => Err(ClientTcpStackError::OutboundClosed),
                Some(std::io::ErrorKind::NetworkUnreachable) => {
                    Err(ClientTcpStackError::DestinationUnreachable)
                }
                _ => Err(ClientTcpStackError::ConnectionReset),
            },
            Err(_) => {
                drop(self.remove_connection(&key).await);
                Err(ClientTcpStackError::ConnectTimeout)
            }
        }
    }

    pub async fn feed(&self, raw_packet: &[u8]) {
        self.feed_correlated(raw_packet).await;
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
        let locked = self.connections.lock().await;
        #[cfg(feature = "packet-memory")]
        let _memory_notifications = self.outbound.defer_memory_notifications();
        let mut connections = locked;
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

    /// Observed Host output retains its actual storage and prepaid transform
    /// envelope through learned PMTU changes and every asynchronous send.
    pub async fn fragment_outbound_packet_buffer(
        &self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<Vec<zero_traits::PacketBuffer>, packet::PacketFragmentFailure> {
        #[cfg(feature = "packet-memory")]
        if self.outbound.has_packet_memory() {
            let Some(mtu) = self.outbound_packet_mtu(&packet).await else {
                return Err(packet::PacketFragmentFailure {
                    cause: zero_traits::PacketDeriveError::LayoutMismatch,
                    original: packet,
                });
            };
            let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
            return packet::fragment_ip_packet_prepaid(packet, mtu, identification);
        }
        // Explicit standalone Vec-channel constructors keep their existing
        // library responsibility. Hosted assembly requires a memory binding
        // before constructing this stack; Unreserved never selects this arm.
        Ok(self
            .fragment_outbound_packet_owned(packet.into_vec())
            .await
            .into_iter()
            .map(Into::into)
            .collect())
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
            .try_send_spec(packet::TcpPacketSpec {
                source: rev.0,
                destination: rev.2,
                source_port: rev.1,
                destination_port: rev.3,
                sequence: conn.snd_nxt.load(Ordering::Acquire),
                acknowledgment: conn.rcv_nxt.load(Ordering::Acquire),
                flags: tcp_flags::ACK,
                window: conn.receive_buffer.window(),
                mss: None,
                payload: &[],
            });
    }

    /// Fence new client flows synchronously. This is not a worker join.
    pub fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }

    /// Fail every original client flow and wake its read/write/connect waiters.
    /// The task owner must still separately join its admitted worker futures.
    async fn remove_connection(&self, key: &ConnKey) -> Option<Conn> {
        let mut connections = self.connections.lock().await;
        let retired = connections.remove(key);
        drop(connections);
        retired
    }
    pub async fn shutdown(&self) {
        self.revoke();
        let locked = self.connections.lock().await;
        #[cfg(feature = "packet-memory")]
        let _memory_notifications = self.outbound.defer_memory_notifications();
        let mut connections = locked;
        for (_, connection) in connections.drain() {
            connection.send_control.observe_transport_closed();
            connection.receive_buffer.close();
        }
    }

    pub async fn cleanup_idle(&self, timeout: Duration) {
        let locked = self.connections.lock().await;
        #[cfg(feature = "packet-memory")]
        let _memory_notifications = self.outbound.defer_memory_notifications();
        let mut connections = locked;
        connections.retain(|_, conn| {
            let keep = conn.last_active.elapsed() < timeout;
            if !keep {
                conn.send_control.expire();
            }
            keep
        });
    }
}
