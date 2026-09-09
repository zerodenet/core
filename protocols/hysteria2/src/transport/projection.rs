use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_FLOW_SCOPE: AtomicU64 = AtomicU64::new(1);

use super::{
    Hysteria2ManagedDatagramFlowResume, Hysteria2ManagedUdpFlowConfig, Hysteria2ManagedUdpFlowPlan,
    Hysteria2ManagedUdpPacketPathCarrierBuild, Hysteria2ManagedUdpPacketPathCarrierDescriptor,
    Hysteria2ManagedUdpPacketPathPlan,
};

impl<'a> Hysteria2ManagedUdpFlowConfig<'a> {
    pub fn with_pool(mut self, pool: &'a super::pool::Hysteria2ConnectionPool) -> Self {
        self.pool = Some(pool);
        self
    }
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
            pool: None,
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
            self.tag.to_owned(),
            self.pool.cloned().unwrap_or_default(),
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
            self.tag.to_owned(),
            self.pool.cloned().unwrap_or_default(),
        )
    }
}

impl Hysteria2ManagedDatagramFlowResume {
    pub(super) fn new(
        protocol: crate::udp::Hysteria2UdpFlowResume,
        tag: String,
        pool: super::pool::Hysteria2ConnectionPool,
    ) -> Self {
        Self {
            protocol,
            cache_scope: NEXT_FLOW_SCOPE.fetch_add(1, Ordering::Relaxed),
            lifetime: std::sync::Arc::new(tokio::sync::watch::channel(()).0),
            tag,
            pool,
        }
    }

    pub(super) fn connector_flow(
        &self,
        server: &str,
        port: u16,
    ) -> crate::udp::Hysteria2UdpConnectorFlow {
        crate::udp::connector_flow_from_resume(&self.protocol, server, port)
            .with_cache_scope(self.cache_scope)
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
    pub(super) fn new(
        protocol: crate::udp::Hysteria2UdpPacketPathCarrierBuild,
        tag: String,
        pool: super::pool::Hysteria2ConnectionPool,
    ) -> Self {
        Self {
            protocol,
            tag,
            pool,
        }
    }
}
