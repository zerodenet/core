//! Actual owned protocol operations preserve prepaid storage through the SDK.
use super::*;

impl RawIpTunnel for WireguardRawIpTunnel {
    fn initiate_handshake_owned(
        &mut self,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<Vec<RawIpAction>> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        owned_result(self.tunnel.initiate_handshake_owned(control), convert)
    }
    fn send_ip_packet_owned(
        &mut self,
        packet: zero_traits::PacketBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<Vec<RawIpAction>> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        owned_result(self.tunnel.send_ip_packet_owned(packet, control), convert)
    }
    fn receive_datagram_owned(
        &mut self,
        source: Option<std::net::SocketAddr>,
        datagram: zero_traits::PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<(Vec<RawIpAction>, bool)> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        owned_result(
            self.tunnel
                .receive_datagram_owned(source, datagram, decode, control),
            |received| (convert(received.actions), received.authenticated),
        )
    }
    fn prepare_datagram_owned(
        &mut self,
        source: Option<std::net::SocketAddr>,
        datagram: zero_traits::PacketBuffer,
        decode: zero_tun::packet_memory::NativeReadBuffer,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<
        crate::runtime::raw_ip::PreparedWireReceive<(Vec<RawIpAction>, bool)>,
    > {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        owned_result(
            self.tunnel
                .prepare_datagram_owned(source, datagram, decode, control),
            |received| match received {
                wireguard::runtime::PreparedReceive::Complete(received) => {
                    crate::runtime::raw_ip::PreparedWireReceive::Complete((
                        convert(received.actions),
                        received.authenticated,
                    ))
                }
                wireguard::runtime::PreparedReceive::Data(data) => {
                    let (identity, packet, peer) = data.into_parts();
                    let allowed = zero_stack::packet::ip_source(&packet).is_some_and(|source| {
                        self.profile
                            .allows_authenticated_source(self.peer_index, source)
                    });
                    crate::runtime::raw_ip::PreparedWireReceive::Data(
                        crate::runtime::raw_ip::PreparedWireData {
                            identity,
                            packet,
                            peer,
                            allowed,
                        },
                    )
                }
            },
        )
    }
    fn commit_prepared_datagram(
        &mut self,
        data: &crate::runtime::raw_ip::PreparedWireData,
    ) -> Result<(), EngineError> {
        self.tunnel
            .commit_prepared_data(&data.identity)
            .map_err(tunnel_error)
    }
    fn tick_owned(
        &mut self,
        control: zero_traits::PacketBuffer,
        memory: &zero_tun::packet_memory::PacketDatagramWorkspaces,
    ) -> crate::runtime::raw_ip::OwnedWireResult<Vec<RawIpAction>> {
        let _read = memory.reads[0].defer_notifications();
        let _decode = memory.reads[1].defer_notifications();
        let _control = memory
            .control
            .as_ref()
            .map(|grant| grant.defer_notifications());
        owned_result(self.tunnel.tick_owned(control), convert)
    }
    fn quiescent(&self) -> bool {
        !self.tunnel.has_pending_packets()
    }
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .initiate_handshake()
            .map(convert)
            .map_err(tunnel_error)
    }

    fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .send_ip_packet(packet)
            .map(convert)
            .map_err(tunnel_error)
    }

    fn receive_datagram(
        &mut self,
        source: Option<std::net::SocketAddr>,
        datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel
            .receive_datagram(source, datagram)
            .map(convert)
            .map_err(tunnel_error)
    }

    fn receive_datagram_with_authentication(
        &mut self,
        source: Option<std::net::SocketAddr>,
        datagram: &[u8],
    ) -> Result<(Vec<RawIpAction>, bool), EngineError> {
        self.tunnel
            .receive_datagram_with_authentication(source, datagram)
            .map(|received| (convert(received.actions), received.authenticated))
            .map_err(tunnel_error)
    }

    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        self.tunnel.tick().map(convert).map_err(tunnel_error)
    }

    fn timer_enabled(&self) -> bool {
        self.tunnel.timer_enabled()
    }

    fn timer_schedule(&self) -> crate::runtime::raw_ip::timer::TimerSchedule {
        self.tunnel.next_timer_delay().into()
    }

    fn allows_source(&self, source: IpAddr) -> bool {
        self.profile
            .allows_authenticated_source(self.peer_index, source)
    }

    fn time_since_last_handshake(&self) -> Option<std::time::Duration> {
        self.tunnel.time_since_last_handshake()
    }
}

fn convert(actions: Vec<TunnelAction>) -> Vec<RawIpAction> {
    actions
        .into_iter()
        .map(|action| match action {
            TunnelAction::SendNetwork(packet) => RawIpAction::SendNetwork(packet),
            TunnelAction::ReceiveIp { packet, source } => RawIpAction::ReceiveIp { packet, source },
        })
        .collect()
}

fn owned_result<T, R>(
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
