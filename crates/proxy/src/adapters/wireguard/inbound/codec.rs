//! Owned and explicit library codec projection; same original engine.
use super::*;

impl RawIpInboundDevice for WireguardInboundDevice {
    fn packet_memory_layout(
        &self,
    ) -> Option<zero_platform_tokio::contract::packet::PacketMemoryLayout> {
        Some(
            zero_platform_tokio::contract::packet::PacketMemoryLayout::Padded {
                multiple: std::num::NonZeroU16::new(16).expect("protocol padding multiple"),
                overhead: 32,
            },
        )
    }
    fn receive_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        packet: zero_traits::PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<RawIpInboundDispatch> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        let output = self
            .0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .receive_datagram_owned(source, packet, decode, &memory.reads[1], control);
        inbound_owned(output, |dispatch| RawIpInboundDispatch {
            peer_index: dispatch.peer_index,
            authenticated: dispatch.authenticated,
            source_rejected_packets: dispatch.source_rejected_packets,
            actions: dispatch.actions.into_iter().map(map_action).collect(),
        })
    }
    fn prepare_datagram_owned(
        &mut self,
        source: Option<SocketAddr>,
        packet: zero_traits::PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<
        crate::runtime::raw_ip::PreparedWireReceive<RawIpInboundDispatch>,
    > {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let mut device = self.0.device.lock().unwrap_or_else(|e| e.into_inner());
        let output =
            device.prepare_datagram_owned(source, packet, decode, &memory.reads[1], control);
        inbound_owned(output, |received| match received {
            wireguard::runtime::PreparedReceive::Complete(dispatch) => {
                crate::runtime::raw_ip::PreparedWireReceive::Complete(RawIpInboundDispatch {
                    peer_index: dispatch.peer_index,
                    authenticated: dispatch.authenticated,
                    source_rejected_packets: dispatch.source_rejected_packets,
                    actions: dispatch.actions.into_iter().map(map_action).collect(),
                })
            }
            wireguard::runtime::PreparedReceive::Data(data) => {
                let (identity, packet, peer) = data.into_parts();
                let allowed = peer
                    .zip(zero_stack::packet::ip_source(&packet))
                    .is_some_and(|(peer, source)| device.prepared_source_allowed(peer, source));
                crate::runtime::raw_ip::PreparedWireReceive::Data(
                    crate::runtime::raw_ip::PreparedWireData {
                        identity,
                        packet,
                        peer,
                        allowed,
                    },
                )
            }
        })
    }
    fn commit_prepared_datagram(
        &mut self,
        data: &crate::runtime::raw_ip::PreparedWireData,
        source: Option<SocketAddr>,
    ) -> Result<(), EngineError> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .commit_prepared_data(
                data.peer
                    .ok_or_else(|| invalid("prepared peer identity missing"))?,
                &data.identity,
                source,
            )
            .map_err(invalid)
    }
    fn send_ip_packet_owned(
        &mut self,
        peer: usize,
        packet: zero_traits::PacketBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<Vec<RawIpInboundAction>> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        let output = self
            .0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send_ip_packet_batch(peer, packet, control);
        inbound_owned(output, |actions| {
            actions.into_iter().map(map_action).collect()
        })
    }
    fn tick_peer_owned(
        &mut self,
        peer: usize,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<Vec<RawIpInboundAction>> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        let output = self
            .0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tick_peer_owned(peer, control);
        inbound_owned(output, |actions| {
            actions.into_iter().map(map_action).collect()
        })
    }
    fn quiescent(&self) -> bool {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .quiescent()
    }

    fn is_local_address(&self, address: IpAddr) -> bool {
        self.0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_local_address(address)
    }
    fn peer_identity(&self, peer: usize) -> Option<String> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .public_peer_id(peer)
    }

    fn authenticated_endpoint(&self, peer: usize) -> Option<SocketAddr> {
        self.0
            .peer_source(peer)
            .and_then(|source| source.authenticated_endpoint)
    }

    fn generation(&self) -> u64 {
        self.0.generation.load(Ordering::Acquire)
    }

    fn mtu(&self) -> u16 {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mtu()
    }

    fn peer_count(&self) -> usize {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_count()
    }

    fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_for_destination(destination)
    }

    fn receive_datagram(
        &mut self,
        source: Option<SocketAddr>,
        packet: &[u8],
    ) -> Result<RawIpInboundDispatch, EngineError> {
        let dispatch = self
            .0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .receive_datagram_with_source(source, packet)
            .map_err(invalid)?;
        Ok(RawIpInboundDispatch {
            peer_index: dispatch.peer_index,
            authenticated: dispatch.authenticated,
            source_rejected_packets: dispatch.source_rejected_packets,
            actions: dispatch.actions.into_iter().map(map_action).collect(),
        })
    }

    fn send_ip_packet(
        &mut self,
        peer: usize,
        packet: &[u8],
    ) -> Result<Vec<RawIpInboundAction>, EngineError> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .send_ip_packet(peer, packet)
            .map(|actions| actions.into_iter().map(map_action).collect())
            .map_err(invalid)
    }

    fn tick_peer(&mut self, peer: usize) -> Result<Vec<RawIpInboundAction>, EngineError> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tick_peer(peer)
            .map(|actions| actions.into_iter().map(map_action).collect())
            .map_err(invalid)
    }

    fn handshake_age(&self, peer: usize) -> Option<std::time::Duration> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .time_since_last_handshake(peer)
    }

    fn timer_schedule(&self, peer: usize) -> crate::runtime::raw_ip::timer::TimerSchedule {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .next_timer_delay(peer)
            .into()
    }
}

fn map_action(action: TunnelAction) -> RawIpInboundAction {
    match action {
        TunnelAction::SendNetwork(packet) => RawIpInboundAction::SendNetwork(packet),
        TunnelAction::ReceiveIp { packet, .. } => RawIpInboundAction::ReceiveIp(packet),
    }
}

fn inbound_owned<T, R>(
    output: wireguard::runtime::OwnedTunnelResult<T>,
    map: impl FnOnce(T) -> R,
) -> crate::runtime::raw_ip::OwnedWireResult<R> {
    crate::runtime::raw_ip::OwnedWireResult {
        result: output.result.map(map).map_err(|source| {
            let storage = source.storage;
            crate::runtime::raw_ip::OwnedWireFailure {
                source: std::io::Error::other(source).into(),
                storage,
            }
        }),
        unused_control: output.unused_control,
    }
}
