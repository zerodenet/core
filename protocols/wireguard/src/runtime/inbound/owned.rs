//! The existing multi-peer verification loop uses one prepaid, reusable scratch
//! allocation. Failed candidates never mutate/copy an uncharged ciphertext.
use super::*;
use crate::runtime::{OwnedTunnelFailure, OwnedTunnelResult};
use zero_traits::{PacketBuffer, PacketDeriveError};

impl InboundDevice {
    pub fn send_ip_packet_owned(
        &mut self,
        peer: usize,
        packet: PacketBuffer,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<Vec<TunnelAction>> {
        let Some(tunnel) = self.peers.get_mut(peer) else {
            return failed(TunnelError::UnknownPeer, None, Some(packet), control);
        };
        let output = tunnel.send_ip_packet_owned(packet, control);
        if let Ok(actions) = &output.result {
            self.register_indices(peer, actions);
        }
        output
    }
    pub fn send_ip_packet_batch(
        &mut self,
        peer: usize,
        packet: PacketBuffer,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<Vec<TunnelAction>> {
        let Some(tunnel) = self.peers.get_mut(peer) else {
            return failed(TunnelError::UnknownPeer, None, Some(packet), control);
        };
        let output = tunnel.send_ip_packet_batch(packet, control);
        if let Ok(actions) = &output.result {
            self.register_indices(peer, actions);
        }
        output
    }
    pub fn tick_peer_owned(
        &mut self,
        peer: usize,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<Vec<TunnelAction>> {
        let Some(tunnel) = self.peers.get_mut(peer) else {
            return failed(TunnelError::UnknownPeer, None, None, control);
        };
        let output = tunnel.tick_owned(control);
        if let Ok(actions) = &output.result {
            self.register_indices(peer, actions);
        }
        output
    }
    pub fn receive_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        mut original: PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        decode_owner: &zero_tun::packet_memory::NativeReadGrant,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<InboundDispatch> {
        if let Err(error) = check_wire_datagram(&original) {
            return failed(error, None, Some(original), control);
        }
        // Only this protocol layer recognizes Data. Admission precedes every
        // SDK/replay/counter mutation. Handshakes and COOKIEs retain their
        // prepaid native read/decode workspaces even when TX is entirely full.
        let data = super::super::owned::is_data_datagram(&original);
        if data {
            if let Err(storage) = original.prepare_transmit(0, 0) {
                return failed(TunnelError::Engine, Some(storage), Some(original), control);
            }
        }
        let original = Arc::new(original);
        let packet = match super::super::owned::receive_scratch(&original, decode, data) {
            Ok(packet) => packet,
            Err(storage) => {
                return failed(
                    TunnelError::Engine,
                    Some(storage),
                    Arc::try_unwrap(original).ok(),
                    control,
                )
            }
        };
        let observed = source.is_some();
        let remote = source.unwrap_or(SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            0,
        )));
        self.rate_limiter.try_reset_count();
        let packet = match self.rate_limiter.verify_packet(remote, packet) {
            Ok(packet) => packet,
            Err(TunnResult::WriteToNetwork(_)) if !observed => {
                return failed(
                    TunnelError::RateLimited,
                    None,
                    Arc::try_unwrap(original).ok(),
                    control,
                );
            }
            Err(TunnResult::WriteToNetwork(cookie)) => {
                return OwnedTunnelResult {
                    result: Ok(InboundDispatch {
                        peer_index: None,
                        authenticated: false,
                        source_rejected_packets: 0,
                        actions: vec![TunnelAction::SendNetwork(super::super::owned_packet(
                            cookie.into(),
                        ))],
                    }),
                    unused_control: Some(control),
                };
            }
            Err(_) => {
                return failed(
                    TunnelError::InvalidWirePacket,
                    None,
                    Arc::try_unwrap(original).ok(),
                    control,
                )
            }
        };
        if !observed
            && matches!(&packet, WgKind::HandshakeInit(_) | WgKind::HandshakeResp(_))
            && !self.unknown_source_handshakes.allow()
        {
            return failed(TunnelError::RateLimited, None, None, control);
        }
        let initial = match &packet {
            WgKind::HandshakeInit(init) => {
                match parse_handshake_anon(&self.static_private, &self.static_public, init) {
                    Ok(handshake) => self.profile.profile.peers.iter().position(|peer| {
                        peer.public_key.as_bytes() == &handshake.peer_static_public
                    }),
                    Err(_) => return failed(TunnelError::InvalidWirePacket, None, None, control),
                }
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
        let initiation = matches!(&packet, WgKind::HandshakeInit(_));
        let candidates = initial
            .into_iter()
            .chain((0..self.peers.len()).filter(|index| !initiation && Some(*index) != initial))
            .collect::<Vec<_>>();
        let mut verified = Some(packet);
        // Keep the SDK's exact control allocation across failed peer candidates.
        // Wrapping it as an outbound EnginePacket between candidates would
        // correctly forbid extracting its Vec again, losing this transfer.
        let control = match Packet::from_owned_buffer(control) {
            Ok(control) => control,
            Err(control) => {
                return failed(
                    TunnelError::Engine,
                    Some(PacketDeriveError::Unreserved),
                    None,
                    control,
                )
            }
        };
        let mut control = Some(control);
        let mut last = TunnelError::UnknownPeer;
        for peer in candidates {
            let packet = match verified.take() {
                Some(packet) => packet,
                // The failed SDK operation has dropped its exact scratch
                // allocation before this next acquisition. No queued packet
                // or still-live candidate is evicted to obtain it.
                None => match decode_owner
                    .try_buffer()
                    .map_err(memory_error)
                    .and_then(|buffer| {
                        super::super::owned::receive_scratch(&original, buffer, data)
                    })
                    .and_then(|packet| {
                        packet
                            .try_into_wg()
                            .map_err(|_| PacketDeriveError::LayoutMismatch)
                    }) {
                    Ok(packet) => packet,
                    Err(storage) => {
                        return failed(
                            TunnelError::Engine,
                            Some(storage),
                            None,
                            super::super::owned_packet(
                                control.take().expect("same unused control"),
                            ),
                        )
                    }
                },
            };
            let tunnel = &mut self.peers[peer];
            if let Err(returned) = tunnel
                .engine
                .install_owned_control(control.take().expect("same actual control"))
            {
                return failed(
                    TunnelError::Engine,
                    Some(PacketDeriveError::LayoutMismatch),
                    None,
                    super::super::owned_packet(returned),
                );
            }
            let result =
                tunnel
                    .handle_verified_packet(packet)
                    .map_err(|error| OwnedTunnelFailure {
                        error,
                        storage: None,
                        original: None,
                    });
            control = tunnel.engine.take_unused_owned_control();
            match result {
                Ok(received) => {
                    self.register_indices(peer, &received.actions);
                    let mut source_rejected_packets = 0;
                    let actions = received
                        .actions
                        .into_iter()
                        .filter(|action| {
                            if let TunnelAction::ReceiveIp { source, .. } = action {
                                if !self
                                    .profile
                                    .routes
                                    .allows_authenticated_source(peer, *source)
                                {
                                    source_rejected_packets += 1;
                                    return false;
                                }
                            }
                            true
                        })
                        .collect();
                    if received.authenticated {
                        self.peer_sources[peer] = PeerSource {
                            last_authenticated_at: Some(Instant::now()),
                            endpoint: source,
                        };
                    }
                    return OwnedTunnelResult {
                        result: Ok(InboundDispatch {
                            peer_index: Some(peer),
                            authenticated: received.authenticated,
                            actions,
                            source_rejected_packets,
                        }),
                        unused_control: control.map(super::super::owned_packet),
                    };
                }
                Err(error) => {
                    last = error.error;
                    if control.is_none() {
                        return OwnedTunnelResult {
                            result: Err(error),
                            unused_control: None,
                        };
                    }
                }
            }
        }
        failed(
            last,
            None,
            Arc::try_unwrap(original).ok(),
            super::super::owned_packet(control.expect("actual unused control after candidates")),
        )
    }
}
pub(super) fn memory_error(error: zero_tun::packet_memory::PacketMemoryError) -> PacketDeriveError {
    use zero_tun::packet_memory::PacketMemoryError;
    match error {
        PacketMemoryError::Capacity => PacketDeriveError::Capacity,
        PacketMemoryError::Busy => PacketDeriveError::Busy,
        PacketMemoryError::Paused | PacketMemoryError::Stopped => PacketDeriveError::Revoked,
        PacketMemoryError::Overflow => PacketDeriveError::Overflow,
    }
}

fn failed<T>(
    error: TunnelError,
    storage: Option<PacketDeriveError>,
    original: Option<PacketBuffer>,
    control: PacketBuffer,
) -> OwnedTunnelResult<T> {
    OwnedTunnelResult {
        result: Err(OwnedTunnelFailure {
            error,
            storage,
            original,
        }),
        unused_control: Some(control),
    }
}

#[cfg(test)]
mod tests;
