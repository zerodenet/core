use alloc::string::String;

use zero_platform_tokio::TokioDatagramSocket;
use zero_transport::StreamTraffic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Socks5UpstreamAssociationCloseReason {
    Closed,
    IdleTimeout,
    Dropped,
}

#[derive(Debug, Clone, Copy)]
pub struct Socks5ManagedUdpFlowConfig<'a> {
    pub(super) protocol: crate::udp::Socks5UdpFlowConfig<'a>,
}

#[derive(Clone)]
pub struct Socks5InboundAcceptor {
    pub(super) protocol: crate::Socks5InboundTcpAcceptor,
}

pub struct Socks5InboundUdpAssociationSetup {
    pub relay: TokioDatagramSocket,
    pub pending_control_traffic: StreamTraffic,
    pub handler: Socks5InboundUdpAssociationHandler,
}

pub struct Socks5InboundUdpAssociationHandler {
    pub(super) protocol: crate::udp::Socks5InboundUdpAssociationSession,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5ManagedUdpAssociationTarget {
    pub(super) protocol: crate::udp::Socks5UdpAssociationTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5ManagedUdpPacketPathCarrierBuild {
    pub(super) protocol: crate::udp::Socks5UdpPacketPathCarrierBuild,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5ManagedUdpPacketPathCarrierDescriptor {
    pub(super) protocol: crate::udp::Socks5UdpPacketPathCarrierDescriptor,
}

#[derive(Debug, Clone)]
pub struct Socks5ManagedUdpFlowPlan {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) association_target: Socks5ManagedUdpAssociationTarget,
}

#[derive(Debug, Clone)]
pub struct Socks5ManagedUdpPacketPathPlan {
    pub(super) tag: String,
    pub(super) carrier_descriptor: Socks5ManagedUdpPacketPathCarrierDescriptor,
    pub(super) carrier_build: Socks5ManagedUdpPacketPathCarrierBuild,
}

#[derive(Debug, Clone)]
pub struct Socks5TransportLeaf {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) username: Option<String>,
    pub(super) password: Option<String>,
}

impl<'a> Socks5ManagedUdpFlowConfig<'a> {
    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        username: Option<&'a str>,
        password: Option<&'a str>,
    ) -> Self {
        Self {
            protocol: crate::udp::Socks5UdpFlowConfig::new(tag, server, port, username, password),
        }
    }

    pub fn association_target(&self) -> Socks5ManagedUdpAssociationTarget {
        Socks5ManagedUdpAssociationTarget::new(self.protocol.association_target())
    }

    pub fn packet_path_carrier_descriptor(&self) -> Socks5ManagedUdpPacketPathCarrierDescriptor {
        Socks5ManagedUdpPacketPathCarrierDescriptor::new(
            self.protocol.packet_path_spec().carrier_descriptor(),
        )
    }

    pub fn packet_path_carrier_build(&self) -> Socks5ManagedUdpPacketPathCarrierBuild {
        Socks5ManagedUdpPacketPathCarrierBuild::new(
            self.protocol.packet_path_spec().carrier_build(),
        )
    }
}

impl Socks5ManagedUdpAssociationTarget {
    pub(super) fn new(protocol: crate::udp::Socks5UdpAssociationTarget) -> Self {
        Self { protocol }
    }

    pub fn outbound_tag(&self) -> &str {
        self.protocol.outbound_tag()
    }

    pub fn log_parts(&self) -> (&str, &str, u16) {
        self.protocol.log_parts()
    }

    pub(super) fn into_protocol_target(self) -> crate::udp::Socks5UdpAssociationTarget {
        self.protocol
    }
}

impl Socks5ManagedUdpPacketPathCarrierBuild {
    pub(super) fn new(protocol: crate::udp::Socks5UdpPacketPathCarrierBuild) -> Self {
        Self { protocol }
    }

    pub(super) fn into_protocol_build(self) -> crate::udp::Socks5UdpPacketPathCarrierBuild {
        self.protocol
    }
}

impl Socks5ManagedUdpPacketPathCarrierDescriptor {
    fn new(protocol: crate::udp::Socks5UdpPacketPathCarrierDescriptor) -> Self {
        Self { protocol }
    }

    pub fn into_parts(self) -> (String, String, u16) {
        self.protocol.into_parts()
    }
}

impl Socks5ManagedUdpFlowPlan {
    pub(super) fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        association_target: Socks5ManagedUdpAssociationTarget,
    ) -> Self {
        Self {
            tag: tag.into(),
            server: server.into(),
            port,
            association_target,
        }
    }

    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn into_parts(self) -> (String, String, u16, Socks5ManagedUdpAssociationTarget) {
        (self.tag, self.server, self.port, self.association_target)
    }

    pub fn into_association_target(self) -> Socks5ManagedUdpAssociationTarget {
        self.association_target
    }
}

impl Socks5ManagedUdpPacketPathPlan {
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub(super) fn new(
        tag: String,
        carrier_descriptor: Socks5ManagedUdpPacketPathCarrierDescriptor,
        carrier_build: Socks5ManagedUdpPacketPathCarrierBuild,
    ) -> Self {
        Self {
            tag,
            carrier_descriptor,
            carrier_build,
        }
    }

    pub fn into_carrier_descriptor(self) -> Socks5ManagedUdpPacketPathCarrierDescriptor {
        self.carrier_descriptor
    }

    pub fn into_carrier_build(self) -> Socks5ManagedUdpPacketPathCarrierBuild {
        self.carrier_build
    }
}
