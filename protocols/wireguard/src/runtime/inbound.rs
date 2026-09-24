use alloc::vec;
use alloc::vec::Vec;
use core::net::IpAddr;

use boringtun::noise::handshake::parse_handshake_anon;
use boringtun::noise::rate_limiter::RateLimiter;
use boringtun::noise::{Packet, TunnResult};
use boringtun::x25519::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

use super::{PeerTunnel, TunnelAction, TunnelError, OUTPUT_BUFFER};
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
        let mut peers = Vec::with_capacity(self.profile.peers.len());
        for peer in 0..self.profile.peers.len() {
            peers.push(PeerTunnel::from_validated_inbound(&self.profile, peer)?);
        }
        let rate_limiter = RateLimiter::new(&static_public, 100);
        Ok(InboundDevice {
            profile: self,
            peers,
            static_private,
            static_public,
            rate_limiter,
        })
    }
}

pub struct InboundDevice {
    profile: PreparedInbound,
    peers: Vec<PeerTunnel>,
    static_private: StaticSecret,
    static_public: PublicKey,
    rate_limiter: RateLimiter,
}

pub struct InboundDispatch {
    /// `None` is a cookie challenge emitted before a peer was authenticated.
    pub peer_index: Option<usize>,
    /// The datagram was accepted by the selected peer's Noise state machine.
    pub authenticated: bool,
    pub actions: Vec<TunnelAction>,
}

impl InboundDevice {
    /// Keep authenticated Noise sessions when their key and timer identity is
    /// unchanged. Receiver indices encode peer positions, so moved peers
    /// must start new sessions.
    pub fn replace_preserving_peers(&mut self, mut next: InboundDevice) -> bool {
        let same_private_key = self.profile.profile.private_key == next.profile.profile.private_key;
        let mut all_retained = same_private_key && self.peers.len() == next.peers.len();
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
                    next.peers[index].set_mtu(next.profile.profile.mtu);
                } else {
                    all_retained = false;
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

    pub fn receive_datagram(
        &mut self,
        source: IpAddr,
        datagram: &[u8],
    ) -> Result<InboundDispatch, TunnelError> {
        let mut buffer = vec![0; OUTPUT_BUFFER];
        let packet = match self
            .rate_limiter
            .verify_packet(Some(source), datagram, &mut buffer)
        {
            Ok(packet) => packet,
            Err(TunnResult::WriteToNetwork(cookie)) => {
                return Ok(InboundDispatch {
                    peer_index: None,
                    authenticated: false,
                    actions: vec![TunnelAction::SendNetwork(cookie.to_vec())],
                });
            }
            Err(_) => return Err(TunnelError::InvalidWirePacket),
        };
        let peer_index = match &packet {
            Packet::HandshakeInit(init) => {
                let handshake =
                    parse_handshake_anon(&self.static_private, &self.static_public, init)
                        .map_err(|_| TunnelError::InvalidWirePacket)?;
                self.profile
                    .profile
                    .peers
                    .iter()
                    .position(|peer| peer.public_key.as_bytes() == &handshake.peer_static_public)
                    .ok_or(TunnelError::UnknownPeer)?
            }
            Packet::HandshakeResponse(response) => (response.receiver_idx >> 8) as usize,
            Packet::PacketCookieReply(cookie) => (cookie.receiver_idx >> 8) as usize,
            Packet::PacketData(data) => (data.receiver_idx >> 8) as usize,
        };
        let peer = self
            .peers
            .get_mut(peer_index)
            .ok_or(TunnelError::UnknownPeer)?;
        let actions = peer.receive_datagram(Some(source), datagram)?;
        let mut checked = Vec::with_capacity(actions.len());
        for action in actions {
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
        Ok(InboundDispatch {
            peer_index: Some(peer_index),
            authenticated: !matches!(packet, Packet::PacketCookieReply(_)),
            actions: checked,
        })
    }

    pub fn send_ip_packet(
        &mut self,
        peer_index: usize,
        packet: &[u8],
    ) -> Result<Vec<TunnelAction>, TunnelError> {
        self.peers
            .get_mut(peer_index)
            .ok_or(TunnelError::UnknownPeer)?
            .send_ip_packet(packet)
    }

    pub fn tick_peer(&mut self, peer_index: usize) -> Result<Vec<TunnelAction>, TunnelError> {
        self.peers
            .get_mut(peer_index)
            .ok_or(TunnelError::UnknownPeer)?
            .tick()
    }
}
