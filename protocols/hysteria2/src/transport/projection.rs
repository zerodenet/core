use zero_core::Session;
use zero_transport::RuntimeError;

use super::{
    connect_hysteria2_tcp_outbound, Hysteria2ManagedDatagramFlowResume,
    Hysteria2ManagedUdpFlowConfig, Hysteria2ManagedUdpFlowPlan,
    Hysteria2ManagedUdpPacketPathCarrierBuild, Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    Hysteria2ManagedUdpPacketPathPlan, Hysteria2OutboundOptionsRef, Hysteria2TransportLeaf,
};

impl<'a> Hysteria2ManagedUdpFlowConfig<'a> {
    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        password: &'a str,
        client_fingerprint: Option<&'a str>,
    ) -> Self {
        Self {
            tag,
            server,
            port,
            password,
            client_fingerprint,
        }
    }

    pub fn flow_resume(&self) -> Hysteria2ManagedDatagramFlowResume {
        Hysteria2ManagedDatagramFlowResume::new(
            crate::udp::Hysteria2UdpFlowConfig::new(
                self.tag,
                self.server,
                self.port,
                self.password,
                self.client_fingerprint,
            )
            .flow_resume(),
        )
    }

    pub fn packet_path_carrier_descriptor(&self) -> Hysteria2ManagedUdpPacketPathCarrierDescriptor {
        Hysteria2ManagedUdpPacketPathCarrierDescriptor::new(
            crate::udp::Hysteria2UdpFlowConfig::new(
                self.tag,
                self.server,
                self.port,
                self.password,
                self.client_fingerprint,
            )
            .packet_path_spec()
            .carrier_descriptor(self.server, self.port),
        )
    }

    pub fn packet_path_carrier_build(&self) -> Hysteria2ManagedUdpPacketPathCarrierBuild {
        Hysteria2ManagedUdpPacketPathCarrierBuild::new(
            crate::udp::Hysteria2UdpFlowConfig::new(
                self.tag,
                self.server,
                self.port,
                self.password,
                self.client_fingerprint,
            )
            .packet_path_spec()
            .carrier_build(self.server, self.port),
        )
    }
}

impl Hysteria2TransportLeaf {
    pub fn from_options_refs(
        tag: &str,
        server: &str,
        port: u16,
        options: Hysteria2OutboundOptionsRef<'_>,
    ) -> Self {
        Self::new(
            tag,
            server,
            port,
            options.password,
            options.client_fingerprint.map(String::from),
        )
    }

    pub fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        password: impl Into<String>,
        client_fingerprint: Option<String>,
    ) -> Self {
        Self {
            tag: tag.into(),
            server: server.into(),
            port,
            password: password.into(),
            client_fingerprint,
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

    pub fn flow_resume(&self) -> Hysteria2ManagedDatagramFlowResume {
        self.flow_config().flow_resume()
    }

    pub fn packet_path_carrier_descriptor(&self) -> Hysteria2ManagedUdpPacketPathCarrierDescriptor {
        self.flow_config().packet_path_carrier_descriptor()
    }

    pub fn packet_path_carrier_build(&self) -> Hysteria2ManagedUdpPacketPathCarrierBuild {
        self.flow_config().packet_path_carrier_build()
    }

    pub fn udp_flow_plan(&self) -> Hysteria2ManagedUdpFlowPlan {
        Hysteria2ManagedUdpFlowPlan::new(
            self.tag.clone(),
            self.server.clone(),
            self.port,
            self.flow_resume(),
        )
    }

    pub fn udp_packet_path_plan(&self) -> Hysteria2ManagedUdpPacketPathPlan {
        Hysteria2ManagedUdpPacketPathPlan::new(
            self.packet_path_carrier_descriptor(),
            self.packet_path_carrier_build(),
        )
    }

    pub async fn open_tcp_stream(
        &self,
        session: &Session,
        sockets: &zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<zero_transport::TcpRelayStream, RuntimeError> {
        connect_hysteria2_tcp_outbound(
            session,
            &self.server,
            self.port,
            &self.password,
            self.client_fingerprint.as_deref(),
            sockets,
        )
        .await
    }

    fn flow_config(&self) -> Hysteria2ManagedUdpFlowConfig<'_> {
        Hysteria2ManagedUdpFlowConfig::new(
            &self.tag,
            &self.server,
            self.port,
            &self.password,
            self.client_fingerprint.as_deref(),
        )
    }
}

impl Hysteria2ManagedDatagramFlowResume {
    fn new(protocol: crate::udp::Hysteria2UdpFlowResume) -> Self {
        Self { protocol }
    }

    pub(super) fn connector_flow(
        &self,
        server: &str,
        port: u16,
    ) -> crate::udp::Hysteria2UdpConnectorFlow {
        crate::udp::connector_flow_from_resume(&self.protocol, server, port)
    }

    pub(super) fn into_protocol_resume(self) -> crate::udp::Hysteria2UdpFlowResume {
        self.protocol
    }
}

impl Hysteria2ManagedUdpFlowPlan {
    fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        resume: Hysteria2ManagedDatagramFlowResume,
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

    pub fn into_parts(self) -> (String, String, u16, Hysteria2ManagedDatagramFlowResume) {
        (self.tag, self.server, self.port, self.resume)
    }

    pub fn into_resume(self) -> Hysteria2ManagedDatagramFlowResume {
        self.resume
    }
}

impl Hysteria2ManagedUdpPacketPathPlan {
    fn new(
        carrier_descriptor: Hysteria2ManagedUdpPacketPathCarrierDescriptor,
        carrier_build: Hysteria2ManagedUdpPacketPathCarrierBuild,
    ) -> Self {
        Self {
            carrier_descriptor,
            carrier_build,
        }
    }

    pub fn into_carrier_descriptor(self) -> Hysteria2ManagedUdpPacketPathCarrierDescriptor {
        self.carrier_descriptor
    }

    pub fn into_carrier_build(self) -> Hysteria2ManagedUdpPacketPathCarrierBuild {
        self.carrier_build
    }
}

impl Hysteria2ManagedUdpPacketPathCarrierDescriptor {
    fn new(protocol: crate::udp::Hysteria2UdpPacketPathCarrierDescriptor) -> Self {
        Self { protocol }
    }

    pub fn into_parts(self) -> (String, String, u16) {
        self.protocol.into_parts()
    }
}

impl Hysteria2ManagedUdpPacketPathCarrierBuild {
    fn new(protocol: crate::udp::Hysteria2UdpPacketPathCarrierBuild) -> Self {
        Self { protocol }
    }

    pub(super) fn into_protocol_build(self) -> crate::udp::Hysteria2UdpPacketPathCarrierBuild {
        self.protocol
    }
}
