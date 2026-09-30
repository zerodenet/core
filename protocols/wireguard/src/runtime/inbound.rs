use alloc::vec;
use alloc::vec::Vec;
use core::net::{IpAddr, SocketAddr};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use gotatun::noise::handshake::parse_handshake_anon;
use gotatun::noise::index_table::IndexTable;
use gotatun::noise::rate_limiter::RateLimiter;
use gotatun::noise::TunnResult;
use gotatun::packet::{Packet, WgKind};
use gotatun::x25519::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use super::{
    check_wire_datagram, PeerTunnel, TunnelAction, TunnelError, UnknownSourceHandshakeLimiter,
};
use crate::routing::PeerRoutes;
use crate::validation::{validate_inbound, InboundInput, InboundValidationError, ValidatedInbound};

pub struct PreparedInbound {
    profile: ValidatedInbound,
    routes: PeerRoutes,
}

impl PreparedInbound {
    pub fn from_input(input: InboundInput<'_>) -> Result<Self, InboundValidationError> {
        let profile = validate_inbound(input)?;
        let routes = PeerRoutes::from_validated_inbound(&profile);
        Ok(Self { profile, routes })
    }

    pub fn mtu(&self) -> u16 {
        self.profile.mtu
    }

    pub fn peer_count(&self) -> usize {
        self.profile.peers.len()
    }

    pub fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.routes.peer_for_destination(destination)
    }

    pub fn into_device(self) -> Result<InboundDevice, TunnelError> {
        let private_bytes = Zeroizing::new(*self.profile.private_key.as_bytes());
        let static_private = StaticSecret::from(*private_bytes);
        let static_public = PublicKey::from(&static_private);
        let index_table = IndexTable::from_os_rng();
        let rate_limiter = Arc::new(RateLimiter::new(&static_public, 100));
        let mut peers = Vec::with_capacity(self.profile.peers.len());
        for peer in 0..self.profile.peers.len() {
            peers.push(PeerTunnel::from_validated_inbound(
                &self.profile,
                peer,
                index_table.clone(),
                Arc::clone(&rate_limiter),
            )?);
        }
        Ok(InboundDevice {
            profile: self,
            peer_sources: vec![PeerSource::default(); peers.len()],
            peers,
            static_private,
            static_public,
            rate_limiter,
            local_indices: HashMap::new(),
            index_order: VecDeque::new(),
            unknown_source_handshakes: UnknownSourceHandshakeLimiter::new(),
        })
    }
}

pub struct InboundDevice {
    profile: PreparedInbound,
    peers: Vec<PeerTunnel>,
    peer_sources: Vec<PeerSource>,
    static_private: StaticSecret,
    static_public: PublicKey,
    rate_limiter: Arc<RateLimiter>,
    local_indices: HashMap<u32, usize>,
    index_order: VecDeque<u32>,
    unknown_source_handshakes: UnknownSourceHandshakeLimiter,
}

#[derive(Clone, Default)]
struct PeerSource {
    last_authenticated_at: Option<Instant>,
    endpoint: Option<SocketAddr>,
}

/// The last source of a packet accepted by this peer's Noise state machine.
/// An opaque carrier can authenticate the peer without reporting a real
/// source address; `source_known` is then false, never an invented endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerSourceObservation {
    pub authenticated_endpoint: Option<SocketAddr>,
    pub source_known: Option<bool>,
    pub last_authenticated_packet_age: Option<core::time::Duration>,
}

pub struct InboundDispatch {
    /// `None` is a cookie challenge emitted before a peer was authenticated.
    pub peer_index: Option<usize>,
    /// The datagram was accepted by the selected peer's Noise state machine.
    pub authenticated: bool,
    pub actions: Vec<TunnelAction>,
}

const MAX_TRACKED_INDICES: usize = 4096;

impl InboundDevice {
    /// Keep authenticated Noise sessions when their key and timer identity is
    /// unchanged. Locally generated receiver indices remain associated with
    /// the same peer position; moved peers must start new sessions.
    pub fn replace_preserving_peers(&mut self, mut next: InboundDevice) -> bool {
        let same_private_key = self.profile.profile.private_key == next.profile.profile.private_key;
        let mut all_retained = same_private_key && self.peers.len() == next.peers.len();
        let mut retained = Vec::new();
        if same_private_key {
            for index in 0..self.peers.len().min(next.peers.len()) {
                let old = &self.profile.profile.peers[index];
                let new = &next.profile.profile.peers[index];
                if old.public_key == new.public_key
                    && old.pre_shared_key == new.pre_shared_key
                    && old.keepalive_secs == new.keepalive_secs
                    && old.reserved == new.reserved
                {
                    core::mem::swap(&mut self.peers[index], &mut next.peers[index]);
                    core::mem::swap(&mut self.peer_sources[index], &mut next.peer_sources[index]);
                    next.peers[index].set_mtu(next.profile.profile.mtu);
                    retained.push(index);
                } else {
                    all_retained = false;
                }
            }
        }
        for index in self.index_order.drain(..) {
            if let Some(&peer) = self.local_indices.get(&index) {
                if retained.contains(&peer) {
                    next.local_indices.insert(index, peer);
                    next.index_order.push_back(index);
                }
            }
        }
        *self = next;
        all_retained
    }

    pub fn mtu(&self) -> u16 {
        self.profile.mtu()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.profile.peer_for_destination(destination)
    }

    pub fn peer_source(&self, peer_index: usize) -> Option<PeerSourceObservation> {
        let source = self.peer_sources.get(peer_index)?;
        Some(PeerSourceObservation {
            authenticated_endpoint: source.endpoint,
            source_known: source
                .last_authenticated_at
                .map(|_| source.endpoint.is_some()),
            last_authenticated_packet_age: source.last_authenticated_at.map(|at| at.elapsed()),
        })
    }

    pub fn receive_datagram(
        &mut self,
        source: SocketAddr,
        datagram: &[u8],
    ) -> Result<InboundDispatch, TunnelError> {
        self.receive_datagram_with_source(Some(source), datagram)
    }

    pub fn receive_datagram_with_source(
        &mut self,
        source: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<InboundDispatch, TunnelError> {
        check_wire_datagram(datagram)?;
        let observed_source = source.is_some();
        let authenticated_source = source;
        let source = source.unwrap_or(SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )));
        self.rate_limiter.try_reset_count();
        let packet = match self
            .rate_limiter
            .verify_packet(source, Packet::copy_from(datagram))
        {
            Ok(packet) => packet,
            // A source-less carrier cannot validate a cookie against a real
            // IP:port. Reject excess handshakes instead of accepting a
            // challenge response for the shared sentinel address.
            Err(TunnResult::WriteToNetwork(_)) if !observed_source => {
                return Err(TunnelError::RateLimited);
            }
            Err(TunnResult::WriteToNetwork(cookie)) => {
                let packet: Packet = cookie.into();
                return Ok(InboundDispatch {
                    peer_index: None,
                    authenticated: false,
                    actions: vec![TunnelAction::SendNetwork(packet.as_ref().to_vec())],
                });
            }
            Err(_) => return Err(TunnelError::InvalidWirePacket),
        };
        if !observed_source
            && matches!(&packet, WgKind::HandshakeInit(_) | WgKind::HandshakeResp(_))
            && !self.unknown_source_handshakes.allow()
        {
            return Err(TunnelError::RateLimited);
        }
        let initial_peer = match &packet {
            WgKind::HandshakeInit(init) => {
                let handshake =
                    parse_handshake_anon(&self.static_private, &self.static_public, init)
                        .map_err(|_| TunnelError::InvalidWirePacket)?;
                Some(
                    self.profile
                        .profile
                        .peers
                        .iter()
                        .position(|peer| {
                            peer.public_key.as_bytes() == &handshake.peer_static_public
                        })
                        .ok_or(TunnelError::UnknownPeer)?,
                )
            }
            WgKind::HandshakeResp(response) => self
                .local_indices
                .get(&response.receiver_idx.get())
                .copied(),
            WgKind::CookieReply(cookie) => {
                self.local_indices.get(&cookie.receiver_idx.get()).copied()
            }
            WgKind::Data(data) => self
                .local_indices
                .get(&data.header.receiver_idx.get())
                .copied(),
        };
        let is_initiation = matches!(&packet, WgKind::HandshakeInit(_));
        let candidates: Vec<_> = initial_peer
            .into_iter()
            .chain(
                (0..self.peers.len())
                    .filter(|index| !is_initiation && Some(*index) != initial_peer),
            )
            .collect();
        let mut verified = Some(packet);
        let mut selected = None;
        let mut last_error = TunnelError::UnknownPeer;
        for peer_index in candidates {
            let packet = match verified.take() {
                Some(packet) => packet,
                None => Packet::copy_from(datagram)
                    .try_into_wg()
                    .map_err(|_| TunnelError::InvalidWirePacket)?,
            };
            match self.peers[peer_index].handle_verified_packet(packet) {
                Ok(received) => {
                    selected = Some((peer_index, received));
                    break;
                }
                Err(error) => last_error = error,
            }
        }
        let (peer_index, received) = selected.ok_or(last_error)?;
        self.register_indices(peer_index, &received.actions);
        let mut checked = Vec::with_capacity(received.actions.len());
        for action in received.actions {
            if let TunnelAction::ReceiveIp { source, .. } = &action {
                if !self
                    .profile
                    .routes
                    .allows_authenticated_source(peer_index, *source)
                {
                    continue;
                }
            }
            checked.push(action);
        }
        if received.authenticated {
            self.peer_sources[peer_index] = PeerSource {
                last_authenticated_at: Some(Instant::now()),
                endpoint: authenticated_source,
            };
        }
        Ok(InboundDispatch {
            peer_index: Some(peer_index),
            authenticated: received.authenticated,
            actions: checked,
        })
    }

    pub fn send_ip_packet(
        &mut self,
        peer_index: usize,
        packet: &[u8],
    ) -> Result<Vec<TunnelAction>, TunnelError> {
        let actions = self
            .peers
            .get_mut(peer_index)
            .ok_or(TunnelError::UnknownPeer)?
            .send_ip_packet(packet)?;
        self.register_indices(peer_index, &actions);
        Ok(actions)
    }

    pub fn tick_peer(&mut self, peer_index: usize) -> Result<Vec<TunnelAction>, TunnelError> {
        let actions = self
            .peers
            .get_mut(peer_index)
            .ok_or(TunnelError::UnknownPeer)?
            .tick()?;
        self.register_indices(peer_index, &actions);
        Ok(actions)
    }

    pub fn time_since_last_handshake(&self, peer_index: usize) -> Option<core::time::Duration> {
        self.peers.get(peer_index)?.time_since_last_handshake()
    }

    fn register_indices(&mut self, peer_index: usize, actions: &[TunnelAction]) {
        for action in actions {
            let TunnelAction::SendNetwork(packet) = action else {
                continue;
            };
            if packet.len() < 8 || !matches!(packet[0], 1 | 2) {
                continue;
            }
            let index = u32::from_le_bytes(
                packet[4..8]
                    .try_into()
                    .expect("sender index length checked"),
            );
            if self.local_indices.insert(index, peer_index).is_none() {
                self.index_order.push_back(index);
            }
            while self.index_order.len() > MAX_TRACKED_INDICES {
                if let Some(oldest) = self.index_order.pop_front() {
                    self.local_indices.remove(&oldest);
                }
            }
        }
    }
}
