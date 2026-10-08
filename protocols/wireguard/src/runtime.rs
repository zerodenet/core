use alloc::vec;
use alloc::vec::Vec;
use core::net::{IpAddr, SocketAddr};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use gotatun::noise::index_table::IndexTable;
use gotatun::noise::rate_limiter::RateLimiter;
use gotatun::noise::{Tunn, TunnResult};
use gotatun::packet::{Packet, WgKind};
use gotatun::tun::MtuWatcher;
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_traits::PacketBuffer;
use zeroize::Zeroizing;

use crate::validation::ValidatedOutbound;

mod buffer;
mod inbound;
mod profile;
use buffer::owned_packet;

pub use inbound::{InboundDevice, InboundDispatch, PeerSourceObservation, PreparedInbound};
pub use profile::{PreparedOutbound, PreparedPeer};

const MAX_WIRE_DATAGRAM: usize = 65_535;
const MAX_DRAIN_RESULTS: usize = 257;
const UNKNOWN_SOURCE_HANDSHAKES_PER_SECOND: u64 = 100;
// A conservative fail-closed ceiling, far below WireGuard's 2^64 message limit.
const MAX_DATA_MESSAGES: u64 = 1 << 32;

fn check_wire_datagram(datagram: &[u8]) -> Result<(), TunnelError> {
    if datagram.is_empty() || datagram.len() > MAX_WIRE_DATAGRAM {
        return Err(TunnelError::InvalidWirePacket);
    }
    if datagram.len() >= 16 && datagram.starts_with(&4_u32.to_le_bytes()) {
        let counter =
            u64::from_le_bytes(datagram[8..16].try_into().expect("counter length checked"));
        if counter >= MAX_DATA_MESSAGES {
            return Err(TunnelError::MessageLimit);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunnelError {
    UnknownPeer,
    TooManyPeers,
    SelfPeer,
    UnsupportedReserved,
    EmptyPacket,
    PacketTooLarge,
    InvalidMtu,
    InvalidWirePacket,
    RateLimited,
    MessageLimit,
    Engine,
    DrainLimit,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TunnelAction {
    SendNetwork(PacketBuffer),
    ReceiveIp {
        packet: PacketBuffer,
        source: IpAddr,
    },
}

pub struct ReceivedDatagram {
    pub actions: Vec<TunnelAction>,
    /// The initial wire datagram was accepted by the peer handshake or data session.
    pub authenticated: bool,
}

/// A cookie for an opaque carrier proves possession of only our sentinel
/// address. Independently cap verified handshakes so a valid MAC2 cannot
/// bypass the unknown-source limit.
pub(super) struct UnknownSourceHandshakeLimiter {
    last_reset: Instant,
    count: u64,
}

impl UnknownSourceHandshakeLimiter {
    fn new() -> Self {
        Self {
            last_reset: Instant::now(),
            count: 0,
        }
    }

    fn allow(&mut self) -> bool {
        if self.last_reset.elapsed() >= Duration::from_secs(1) {
            self.last_reset = Instant::now();
            self.count = 0;
        }
        self.count = self.count.saturating_add(1);
        self.count <= UNKNOWN_SOURCE_HANDSHAKES_PER_SECOND
    }
}

/// One authenticated WireGuard peer. The caller owns UDP I/O and periodic ticks.
/// No Tokio socket, task, endpoint, or private configuration string is retained.
pub struct PeerTunnel {
    engine: Tunn,
    rate_limiter: Arc<RateLimiter>,
    mtu: usize,
    emitted_data_messages: u64,
    unknown_source_handshakes: UnknownSourceHandshakeLimiter,
}

impl PeerTunnel {
    fn set_mtu(&mut self, mtu: u16) {
        self.mtu = usize::from(mtu);
    }

    pub fn from_validated(
        profile: &ValidatedOutbound<'_>,
        peer_index: usize,
    ) -> Result<Self, TunnelError> {
        let peer = profile
            .peers
            .get(peer_index)
            .ok_or(TunnelError::UnknownPeer)?;
        Self::from_keys(
            &profile.private_key,
            profile.mtu,
            &peer.public_key,
            peer.pre_shared_key.as_ref(),
            peer.keepalive_secs,
            peer.reserved,
            None,
        )
    }

    pub fn from_prepared(
        profile: &PreparedOutbound,
        peer_index: usize,
    ) -> Result<Self, TunnelError> {
        let peer = profile.peer(peer_index).ok_or(TunnelError::UnknownPeer)?;
        Self::from_keys(
            profile.private_key(),
            profile.mtu(),
            peer.public_key(),
            peer.pre_shared_key(),
            peer.keepalive_secs(),
            peer.reserved(),
            None,
        )
    }

    fn from_validated_inbound(
        profile: &crate::validation::ValidatedInbound,
        peer_index: usize,
        index_table: IndexTable,
        rate_limiter: Arc<RateLimiter>,
    ) -> Result<Self, TunnelError> {
        let peer = profile
            .peers
            .get(peer_index)
            .ok_or(TunnelError::UnknownPeer)?;
        Self::from_keys(
            &profile.private_key,
            profile.mtu,
            &peer.public_key,
            peer.pre_shared_key.as_ref(),
            peer.keepalive_secs,
            peer.reserved,
            Some((index_table, rate_limiter)),
        )
    }

    fn from_keys(
        private_key: &crate::validation::Key,
        mtu: u16,
        public_key: &crate::validation::Key,
        pre_shared_key: Option<&crate::validation::Key>,
        keepalive_secs: u16,
        reserved: Option<[u8; 3]>,
        context: Option<(IndexTable, Arc<RateLimiter>)>,
    ) -> Result<Self, TunnelError> {
        if reserved.is_some_and(|reserved| reserved != [0; 3]) {
            return Err(TunnelError::UnsupportedReserved);
        }
        if mtu > crate::validation::MAX_MTU {
            return Err(TunnelError::InvalidMtu);
        }

        let private_bytes = Zeroizing::new(*private_key.as_bytes());
        let static_private = StaticSecret::from(*private_bytes);
        let static_public = PublicKey::from(&static_private);
        if static_public.as_bytes() == public_key.as_bytes() {
            return Err(TunnelError::SelfPeer);
        }
        let peer_public = PublicKey::from(*public_key.as_bytes());
        let pre_shared = pre_shared_key.map(|key| Zeroizing::new(*key.as_bytes()));
        let keepalive = (keepalive_secs != 0).then_some(keepalive_secs);
        let (index_table, rate_limiter) = context.unwrap_or_else(|| {
            (
                IndexTable::from_os_rng(),
                Arc::new(RateLimiter::new(&static_public, 100)),
            )
        });

        Ok(Self {
            engine: Tunn::new(
                static_private,
                peer_public,
                pre_shared.as_ref().map(|key| **key),
                keepalive,
                index_table,
                Arc::clone(&rate_limiter),
            ),
            rate_limiter,
            mtu: usize::from(mtu),
            emitted_data_messages: 0,
            unknown_source_handshakes: UnknownSourceHandshakeLimiter::new(),
        })
    }

    pub fn initiate_handshake(&mut self) -> Result<Vec<TunnelAction>, TunnelError> {
        self.engine
            .format_handshake_initiation(false)
            .map(|packet| self.collect_result(TunnResult::WriteToNetwork(packet.into())))
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Encrypt one complete inner IP packet. Transport packets are padded to a
    /// 16-byte plaintext boundary before the protocol engine encrypts them.
    pub fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<TunnelAction>, TunnelError> {
        if packet.is_empty() {
            return Err(TunnelError::EmptyPacket);
        }
        if packet.len() > self.mtu {
            return Err(TunnelError::PacketTooLarge);
        }
        if self.emitted_data_messages >= MAX_DATA_MESSAGES {
            return Err(TunnelError::MessageLimit);
        }
        let padded_len = packet.len().div_ceil(16) * 16;
        let mut padded = Packet::default();
        padded.buf_mut().reserve(padded_len);
        padded.buf_mut().extend_from_slice(packet);
        padded.buf_mut().resize(padded_len, 0);
        let result = self.engine.handle_outgoing_packet(padded, None);
        result
            .map(|packet| self.collect_result(TunnResult::WriteToNetwork(packet)))
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    /// Process one UDP datagram from the peer endpoint, then drain packets
    /// queued by a completed handshake.
    pub fn receive_datagram(
        &mut self,
        source: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<Vec<TunnelAction>, TunnelError> {
        self.receive_datagram_with_authentication(source, datagram)
            .map(|received| received.actions)
    }

    pub fn receive_datagram_with_authentication(
        &mut self,
        source: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<ReceivedDatagram, TunnelError> {
        check_wire_datagram(datagram)?;
        let observed_source = source.is_some();
        let source = source.unwrap_or(SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )));
        self.rate_limiter.try_reset_count();
        let verified = self
            .rate_limiter
            .verify_packet(source, Packet::copy_from(datagram));
        match verified {
            Ok(packet) => {
                if !observed_source
                    && matches!(&packet, WgKind::HandshakeInit(_) | WgKind::HandshakeResp(_))
                    && !self.unknown_source_handshakes.allow()
                {
                    return Err(TunnelError::RateLimited);
                }
                self.handle_verified_packet(packet)
            }
            // A cookie tied to the sentinel cannot prove possession of a
            // real source address. Opaque carriers fail closed under load.
            Err(TunnResult::WriteToNetwork(_)) if !observed_source => Err(TunnelError::RateLimited),
            Err(TunnResult::WriteToNetwork(cookie)) => Ok(ReceivedDatagram {
                actions: self.collect_result(TunnResult::WriteToNetwork(cookie))?,
                authenticated: false,
            }),
            Err(_) => Err(TunnelError::InvalidWirePacket),
        }
    }

    fn handle_verified_packet(&mut self, packet: WgKind) -> Result<ReceivedDatagram, TunnelError> {
        let is_response = matches!(&packet, WgKind::HandshakeResp(_));
        let is_cookie = matches!(&packet, WgKind::CookieReply(_));
        let result = self.engine.handle_incoming_packet(packet);
        let authenticated = match &result {
            TunnResult::WriteToNetwork(WgKind::HandshakeResp(_) | WgKind::Data(_)) => !is_cookie,
            TunnResult::WriteToTunnel(_) => true,
            _ => false,
        };
        let mut actions = self.collect_result(result)?;
        if is_response {
            let mut mtu = MtuWatcher::new(self.mtu as u16);
            let queued: Vec<_> = self.engine.get_queued_packets(&mut mtu).collect();
            for (count, packet) in queued.into_iter().enumerate() {
                if count >= MAX_DRAIN_RESULTS {
                    return Err(TunnelError::DrainLimit);
                }
                actions.extend(self.collect_result(TunnResult::WriteToNetwork(packet))?);
            }
        }
        Ok(ReceivedDatagram {
            actions,
            authenticated,
        })
    }

    pub fn tick(&mut self) -> Result<Vec<TunnelAction>, TunnelError> {
        match self.engine.update_timers() {
            Ok(Some(packet)) => self.collect_result(TunnResult::WriteToNetwork(packet)),
            Ok(None) => Ok(Vec::new()),
            Err(_) => Err(TunnelError::Engine),
        }
    }

    pub fn time_since_last_handshake(&self) -> Option<Duration> {
        self.engine.time_since_last_handshake()
    }

    fn collect_result(&mut self, result: TunnResult) -> Result<Vec<TunnelAction>, TunnelError> {
        match result {
            TunnResult::Done => Ok(Vec::new()),
            TunnResult::Err(_) => Err(TunnelError::Engine),
            TunnResult::WriteToNetwork(packet) => {
                if matches!(&packet, WgKind::Data(_)) {
                    if self.emitted_data_messages >= MAX_DATA_MESSAGES {
                        return Err(TunnelError::MessageLimit);
                    }
                    self.emitted_data_messages += 1;
                }
                let packet: Packet = packet.into();
                Ok(vec![TunnelAction::SendNetwork(owned_packet(packet))])
            }
            TunnResult::WriteToTunnel(packet) if packet.is_empty() => Ok(Vec::new()),
            TunnResult::WriteToTunnel(packet) => {
                let packet = packet
                    .try_into_ip()
                    .map_err(|_| TunnelError::InvalidWirePacket)?;
                let source = packet.source().ok_or(TunnelError::InvalidWirePacket)?;
                let packet = packet
                    .try_into_ipvx()
                    .map_err(|_| TunnelError::InvalidWirePacket)?
                    .either(
                        |packet| owned_packet(packet.into_bytes()),
                        |packet| owned_packet(packet.into_bytes()),
                    );
                Ok(vec![TunnelAction::ReceiveIp { packet, source }])
            }
        }
    }
}
