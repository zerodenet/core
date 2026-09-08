use super::{
    Hysteria2ManagedDatagramFlowResume, Hysteria2ManagedUdpFlowConfig, Hysteria2ManagedUdpFlowPlan,
    Hysteria2ManagedUdpPacketPathCarrierBuild, Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    Hysteria2ManagedUdpPacketPathPlan,
};

impl<'a> Hysteria2ManagedUdpFlowConfig<'a> {
    pub fn with_settings(mut self, settings: crate::settings::Settings) -> Self {
        self.settings = settings;
        self
    }
    pub fn with_server_name(mut self, server_name: Option<&'a str>) -> Self {
        self.server_name = server_name;
        self
    }

    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

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
            insecure: false,
            server_name: None,
            settings: Default::default(),
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
            .with_settings(self.settings)
            .with_server_name(self.server_name)
            .with_insecure(self.insecure)
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
            .with_settings(self.settings)
            .with_server_name(self.server_name)
            .with_insecure(self.insecure)
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
            .with_settings(self.settings)
            .with_server_name(self.server_name)
            .with_insecure(self.insecure)
            .packet_path_spec()
            .carrier_build(self.server, self.port),
        )
    }
}

impl Hysteria2ManagedDatagramFlowResume {
    pub(super) fn new(protocol: crate::udp::Hysteria2UdpFlowResume) -> Self {
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
    pub(super) fn new(
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
    pub(super) fn new(
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
    pub(super) fn new(protocol: crate::udp::Hysteria2UdpPacketPathCarrierDescriptor) -> Self {
        Self { protocol }
    }

    pub fn into_parts(self) -> (String, String, u16) {
        self.protocol.into_parts()
    }
}

impl Hysteria2ManagedUdpPacketPathCarrierBuild {
    pub(super) fn new(protocol: crate::udp::Hysteria2UdpPacketPathCarrierBuild) -> Self {
        Self { protocol }
    }

    pub(super) fn into_protocol_build(self) -> crate::udp::Hysteria2UdpPacketPathCarrierBuild {
        self.protocol
    }
}
