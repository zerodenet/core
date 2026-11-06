use std::sync::Arc;

use zero_core::Address;
use zero_traits::DatagramCodec;

pub type ShadowsocksUdpResponse = (Address, u16, Vec<u8>);

#[derive(Debug, Clone)]
pub struct ShadowsocksManagedDatagramFlowResume {
    pub(crate) root: Option<crate::component::udp::PoolSelection>,
    pub(super) plugin: Option<super::plugin::outbound::PluginPlan>,
    pub(super) protocol: crate::udp::ShadowsocksUdpFlowResume,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksManagedUdpPacketPathCarrierDescriptor {
    pub(super) protocol: crate::udp::ShadowsocksUdpPacketPathCarrierDescriptor,
}

#[derive(Debug, Clone)]
pub struct ShadowsocksManagedUdpPacketPathDatagramSourceBuild {
    pub(super) protocol: crate::udp::ShadowsocksUdpPacketPathDatagramSourceBuild,
}

#[derive(Debug, Clone)]
pub struct ShadowsocksManagedUdpFlowPlan {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) resume: ShadowsocksManagedDatagramFlowResume,
}

#[derive(Clone)]
pub struct ShadowsocksManagedUdpPacketPathPlan {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) carrier_descriptor: ShadowsocksManagedUdpPacketPathCarrierDescriptor,
    pub(super) spec: crate::udp::ShadowsocksUdpPacketPathSpec,
}

#[derive(Clone)]
pub struct ShadowsocksManagedUdpFlowConfig<'a> {
    pub(super) limits: crate::validation::StateLimits,
    pub(super) tag: &'a str,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) cipher: &'a str,
    pub(super) password: &'a str,
    pub(super) replay: crate::shared::legacy_replay::LegacyReplay,
}

#[derive(Clone)]
pub struct ShadowsocksTransportLeaf {
    pub(super) limits: crate::validation::StateLimits,
    pub(super) udp_root: Option<crate::component::udp::PoolSelection>,
    pub(super) plugin: Option<super::plugin::outbound::PluginPlan>,
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) cipher: String,
    pub(super) password: String,
    pub(super) replay: crate::shared::legacy_replay::LegacyReplay,
}

#[derive(Debug, Clone)]
pub(crate) struct ShadowsocksInboundProfile {
    pub(super) protocol: crate::ShadowsocksInboundProfile,
}

#[derive(Clone)]
pub struct ShadowsocksInboundTcpAcceptor {
    pub(super) protocol: crate::ShadowsocksInboundTcpAcceptor,
}

pub struct ShadowsocksInboundBindings {
    pub(super) acceptor: ShadowsocksInboundTcpAcceptor,
    pub(super) udp_relay: crate::udp::ShadowsocksInboundUdpRelay,
}

mod flow_config;

impl ShadowsocksManagedDatagramFlowResume {
    fn new(protocol: crate::udp::ShadowsocksUdpFlowResume) -> Self {
        Self {
            protocol,
            plugin: None,
            root: None,
        }
    }

    pub(super) fn socket_flow_spec(&self) -> crate::udp::ShadowsocksUdpSocketFlowSpec {
        crate::udp::managed_socket_flow_from_resume(&self.protocol)
    }

    pub(super) fn into_shared_managed_socket_flow_codec(
        self,
    ) -> Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> {
        self.protocol.into_shared_managed_socket_flow_codec()
    }
}

impl ShadowsocksManagedUdpFlowPlan {
    pub(super) fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        resume: ShadowsocksManagedDatagramFlowResume,
    ) -> Self {
        Self {
            tag: tag.into(),
            server: server.into(),
            port,
            resume,
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

    pub fn into_parts(self) -> (String, String, u16, ShadowsocksManagedDatagramFlowResume) {
        (self.tag, self.server, self.port, self.resume)
    }

    pub fn into_resume(self) -> ShadowsocksManagedDatagramFlowResume {
        self.resume
    }
}

impl ShadowsocksManagedUdpPacketPathPlan {
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub(super) fn new(
        server: impl Into<String>,
        port: u16,
        carrier_descriptor: ShadowsocksManagedUdpPacketPathCarrierDescriptor,
        tag: String,
        spec: crate::udp::ShadowsocksUdpPacketPathSpec,
    ) -> Self {
        Self {
            server: server.into(),
            port,
            carrier_descriptor,
            tag,
            spec,
        }
    }

    pub fn server(&self) -> &str {
        &self.server
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn carrier_codec(&self) -> Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> {
        self.spec.carrier_codec()
    }

    pub fn into_carrier_descriptor(self) -> ShadowsocksManagedUdpPacketPathCarrierDescriptor {
        self.carrier_descriptor
    }

    pub fn into_datagram_source_build(self) -> ShadowsocksManagedUdpPacketPathDatagramSourceBuild {
        ShadowsocksManagedUdpPacketPathDatagramSourceBuild::new(self.spec.datagram_source_build(
            &self.tag,
            &self.server,
            self.port,
        ))
    }
}

impl ShadowsocksManagedUdpPacketPathCarrierDescriptor {
    fn new(protocol: crate::udp::ShadowsocksUdpPacketPathCarrierDescriptor) -> Self {
        Self { protocol }
    }

    pub fn into_parts(self) -> (String, String, u16) {
        self.protocol.into_parts()
    }
}

impl ShadowsocksManagedUdpPacketPathDatagramSourceBuild {
    fn new(protocol: crate::udp::ShadowsocksUdpPacketPathDatagramSourceBuild) -> Self {
        Self { protocol }
    }

    pub fn into_shared_codec_parts(
        self,
    ) -> (
        String,
        String,
        u16,
        String,
        Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    ) {
        self.protocol.into_shared_codec_parts()
    }
}

impl core::fmt::Debug for ShadowsocksManagedUdpFlowConfig<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksManagedUdpFlowConfig")
            .field("cipher", &self.cipher)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for ShadowsocksTransportLeaf {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksTransportLeaf")
            .field("cipher", &self.cipher)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("limits", &self.limits)
            .field("plugin", &self.plugin)
            .finish_non_exhaustive()
    }
}
