use super::*;

impl Hysteria2TransportLeaf {
    pub fn with_pool(mut self, pool: super::pool::Hysteria2ConnectionPool) -> Self {
        self.pool = pool;
        self
    }
    pub fn with_settings(mut self, settings: crate::settings::Settings) -> Self {
        self.settings = settings;
        self
    }
    pub fn with_server_name(mut self, server_name: Option<&str>) -> Self {
        self.server_name = server_name.map(ToOwned::to_owned);
        self
    }

    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

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
        .with_settings(options.settings)
        .with_server_name(options.server_name)
        .with_insecure(options.insecure)
    }

    pub fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        password: impl Into<String>,
        client_fingerprint: Option<String>,
    ) -> Self {
        Self {
            pool: Default::default(),
            tag: tag.into(),
            server: server.into(),
            port,
            password: password.into(),
            client_fingerprint,
            insecure: false,
            server_name: None,
            settings: Default::default(),
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
        super::pool::connect_pooled(
            &self.pool,
            &self.tag,
            session,
            &self.server,
            self.port,
            Hysteria2OutboundOptionsRef {
                settings: self.settings,
                password: &self.password,
                client_fingerprint: self.client_fingerprint.as_deref(),
                insecure: self.insecure,
                server_name: self.server_name.as_deref(),
            },
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
        .with_settings(self.settings)
        .with_server_name(self.server_name.as_deref())
        .with_insecure(self.insecure)
    }
}
