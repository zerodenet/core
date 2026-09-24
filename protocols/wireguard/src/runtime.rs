use alloc::vec;
use alloc::vec::Vec;
use core::net::IpAddr;
use std::time::Duration;

use boringtun::noise::{Tunn, TunnResult};
use boringtun::x25519::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use crate::validation::ValidatedOutbound;

mod inbound;
mod profile;

pub use inbound::{InboundDevice, InboundDispatch, PreparedInbound};
pub use profile::{PreparedOutbound, PreparedPeer};

const MAX_WIRE_DATAGRAM: usize = 65_535;
const OUTPUT_BUFFER: usize = MAX_WIRE_DATAGRAM + 32;
const MAX_DRAIN_RESULTS: usize = 257;
// A conservative fail-closed ceiling, far below WireGuard's 2^64 message limit.
const MAX_DATA_MESSAGES: u64 = 1 << 32;

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
    MessageLimit,
    Engine,
    DrainLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunnelAction {
    SendNetwork(Vec<u8>),
    ReceiveIp { packet: Vec<u8>, source: IpAddr },
}

pub struct ReceivedDatagram {
    pub actions: Vec<TunnelAction>,
    /// The initial wire datagram was accepted by the peer handshake or data session.
    pub authenticated: bool,
}

/// One authenticated WireGuard peer. The caller owns UDP I/O and periodic ticks.
/// No Tokio socket, task, endpoint, or private configuration string is retained.
pub struct PeerTunnel {
    engine: Tunn,
    mtu: usize,
    emitted_data_messages: u64,
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
            peer_index,
            &peer.public_key,
            peer.pre_shared_key.as_ref(),
            peer.keepalive_secs,
            peer.reserved,
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
            peer_index,
            peer.public_key(),
            peer.pre_shared_key(),
            peer.keepalive_secs(),
            peer.reserved(),
        )
    }

    fn from_validated_inbound(
        profile: &crate::validation::ValidatedInbound,
        peer_index: usize,
    ) -> Result<Self, TunnelError> {
        let peer = profile
            .peers
            .get(peer_index)
            .ok_or(TunnelError::UnknownPeer)?;
        Self::from_keys(
            &profile.private_key,
            profile.mtu,
            peer_index,
            &peer.public_key,
            peer.pre_shared_key.as_ref(),
            peer.keepalive_secs,
            peer.reserved,
        )
    }

    fn from_keys(
        private_key: &crate::validation::Key,
        mtu: u16,
        peer_index: usize,
        public_key: &crate::validation::Key,
        pre_shared_key: Option<&crate::validation::Key>,
        keepalive_secs: u16,
        reserved: Option<[u8; 3]>,
    ) -> Result<Self, TunnelError> {
        let index = u32::try_from(peer_index).map_err(|_| TunnelError::TooManyPeers)?;
        if index > 0x00ff_ffff {
            return Err(TunnelError::TooManyPeers);
        }
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

        Ok(Self {
            engine: Tunn::new(
                static_private,
                peer_public,
                pre_shared.as_ref().map(|key| **key),
                keepalive,
                index,
                None,
            ),
            mtu: usize::from(mtu),
            emitted_data_messages: 0,
        })
    }

    pub fn initiate_handshake(&mut self) -> Result<Vec<TunnelAction>, TunnelError> {
        let mut buffer = vec![0; OUTPUT_BUFFER];
        let result = self.engine.format_handshake_initiation(&mut buffer, false);
        self.collect_result(result)
    }

    /// Encrypt one complete inner IP packet. Transport packets are padded to a
    /// 16-byte plaintext boundary before BoringTun encrypts them.
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
        let mut padded = Zeroizing::new(vec![0_u8; padded_len]);
        padded[..packet.len()].copy_from_slice(packet);
        let mut buffer = vec![0; OUTPUT_BUFFER];
        let result = self.engine.encapsulate(&padded, &mut buffer);
        self.collect_result(result)
    }

    /// Process one UDP datagram from the peer endpoint, then drain packets
    /// queued by a completed handshake.
    pub fn receive_datagram(
        &mut self,
        source: Option<IpAddr>,
        datagram: &[u8],
    ) -> Result<Vec<TunnelAction>, TunnelError> {
        self.receive_datagram_with_authentication(source, datagram)
            .map(|received| received.actions)
    }

    pub fn receive_datagram_with_authentication(
        &mut self,
        source: Option<IpAddr>,
        datagram: &[u8],
    ) -> Result<ReceivedDatagram, TunnelError> {
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
        let mut buffer = vec![0; OUTPUT_BUFFER];
        let result = self.engine.decapsulate(source, datagram, &mut buffer);
        let authenticated = match (datagram.first().copied(), &result) {
            (Some(1), TunnResult::WriteToNetwork(packet)) => {
                packet.starts_with(&2_u32.to_le_bytes())
            }
            (Some(2), TunnResult::WriteToNetwork(packet)) => {
                packet.starts_with(&4_u32.to_le_bytes())
            }
            (
                Some(4),
                TunnResult::Done
                | TunnResult::WriteToTunnelV4(..)
                | TunnResult::WriteToTunnelV6(..),
            ) => true,
            _ => false,
        };
        let mut actions = self.collect_result(result)?;
        for _ in 0..MAX_DRAIN_RESULTS {
            let mut buffer = vec![0; OUTPUT_BUFFER];
            match self.engine.decapsulate(source, &[], &mut buffer) {
                TunnResult::Done => {
                    return Ok(ReceivedDatagram {
                        actions,
                        authenticated,
                    })
                }
                result => actions.extend(self.collect_result(result)?),
            }
        }
        Err(TunnelError::DrainLimit)
    }

    pub fn tick(&mut self) -> Result<Vec<TunnelAction>, TunnelError> {
        let mut buffer = vec![0; OUTPUT_BUFFER];
        let result = self.engine.update_timers(&mut buffer);
        self.collect_result(result)
    }

    pub fn time_since_last_handshake(&self) -> Option<Duration> {
        self.engine.time_since_last_handshake()
    }

    fn collect_result(&mut self, result: TunnResult<'_>) -> Result<Vec<TunnelAction>, TunnelError> {
        match result {
            TunnResult::Done => Ok(Vec::new()),
            TunnResult::Err(_) => Err(TunnelError::Engine),
            TunnResult::WriteToNetwork(packet) => {
                if packet.starts_with(&4_u32.to_le_bytes()) {
                    if self.emitted_data_messages >= MAX_DATA_MESSAGES {
                        return Err(TunnelError::MessageLimit);
                    }
                    self.emitted_data_messages += 1;
                }
                Ok(vec![TunnelAction::SendNetwork(packet.to_vec())])
            }
            TunnResult::WriteToTunnelV4(packet, source) => Ok(vec![TunnelAction::ReceiveIp {
                packet: packet.to_vec(),
                source: IpAddr::V4(source),
            }]),
            TunnResult::WriteToTunnelV6(packet, source) => Ok(vec![TunnelAction::ReceiveIp {
                packet: packet.to_vec(),
                source: IpAddr::V6(source),
            }]),
        }
    }
}
