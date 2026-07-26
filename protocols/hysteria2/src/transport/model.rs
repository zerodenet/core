pub struct QuicConnectionOptions<'a> {
    pub server: &'a str,
    pub port: u16,
    pub alpn: Vec<Vec<u8>>,
    pub quic_profile: Hysteria2QuicProfile,
    pub datagram_receive_buffer_size: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedDatagramFlowResume {
    pub(super) protocol: crate::udp::Hysteria2UdpFlowResume,
}

#[derive(Debug, Clone)]
pub struct Hysteria2AuthenticatedInboundProfile {
    pub(super) protocol: crate::inbound::Hysteria2InboundProfile,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Hysteria2InboundTcpResponseProtocol {
    pub(super) protocol: crate::inbound::Hysteria2InboundTcpAcceptor,
}

pub struct Hysteria2AuthenticatedQuicConnection {
    pub(super) protocol: crate::inbound::Hysteria2AcceptedQuicConnection,
    pub(super) _http3: Option<super::auth::Hysteria2Http3ServerGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2ManagedUdpPacketPathCarrierDescriptor {
    pub(super) protocol: crate::udp::Hysteria2UdpPacketPathCarrierDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2ManagedUdpPacketPathCarrierBuild {
    pub(super) protocol: crate::udp::Hysteria2UdpPacketPathCarrierBuild,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpFlowPlan {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) resume: Hysteria2ManagedDatagramFlowResume,
}

#[derive(Debug, Clone)]
pub struct Hysteria2ManagedUdpPacketPathPlan {
    pub(super) carrier_descriptor: Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    pub(super) carrier_build: Hysteria2ManagedUdpPacketPathCarrierBuild,
}

#[derive(Debug, Clone, Copy)]
pub struct Hysteria2ManagedUdpFlowConfig<'a> {
    pub(super) tag: &'a str,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) password: &'a str,
    pub(super) client_fingerprint: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct Hysteria2TransportLeaf {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) password: String,
    pub(super) client_fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2QuicProfile {
    pub(super) client_fingerprint: Option<String>,
}
