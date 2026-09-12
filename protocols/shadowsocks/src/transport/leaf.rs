use core::future::Future;
use std::sync::Arc;

use zero_core::{Address, Session};
use zero_platform_tokio::TokioSocket;
use zero_traits::DatagramCodec;
use zero_transport::RuntimeError;
use zero_transport::{MeteredStream, StreamTraffic, TcpRelayStream};

use super::{
    ShadowsocksManagedDatagramFlowResume, ShadowsocksManagedUdpFlowConfig,
    ShadowsocksManagedUdpFlowPlan, ShadowsocksManagedUdpPacketPathCarrierDescriptor,
    ShadowsocksManagedUdpPacketPathDatagramSourceBuild, ShadowsocksManagedUdpPacketPathPlan,
    ShadowsocksOutboundOptionsRef, ShadowsocksTransportLeaf,
};

impl ShadowsocksTransportLeaf {
    pub fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        self
    }
    pub fn from_options_refs(
        tag: &str,
        server: &str,
        port: u16,
        options: ShadowsocksOutboundOptionsRef<'_>,
    ) -> Self {
        Self::new(tag, server, port, options.cipher, options.password)
    }

    pub fn new(
        tag: impl Into<String>,
        server: impl Into<String>,
        port: u16,
        cipher: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            plugin: None,
            limits: Default::default(),
            tag: tag.into(),
            server: server.into(),
            port,
            cipher: cipher.into(),
            password: password.into(),
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
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

    pub fn cipher(&self) -> &str {
        &self.cipher
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn flow_resume(&self) -> Result<ShadowsocksManagedDatagramFlowResume, zero_core::Error> {
        let mut resume = self.flow_config().flow_resume()?;
        if let Some(plugin) = &self.plugin {
            resume.protocol = resume
                .protocol
                .with_carrier_identity(&plugin.cache_identity());
            resume.plugin = Some(plugin.clone());
        }
        Ok(resume)
    }

    pub fn packet_path_carrier_descriptor(
        &self,
    ) -> Result<ShadowsocksManagedUdpPacketPathCarrierDescriptor, zero_core::Error> {
        self.flow_config().packet_path_carrier_descriptor()
    }

    pub fn packet_path_carrier_codec(
        &self,
    ) -> Result<Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>, zero_core::Error> {
        self.flow_config().packet_path_carrier_codec()
    }

    pub fn packet_path_datagram_source_build(
        &self,
    ) -> Result<ShadowsocksManagedUdpPacketPathDatagramSourceBuild, zero_core::Error> {
        self.flow_config().packet_path_datagram_source_build()
    }

    pub fn udp_flow_plan(&self) -> Result<ShadowsocksManagedUdpFlowPlan, zero_core::Error> {
        Ok(ShadowsocksManagedUdpFlowPlan::new(
            self.tag.clone(),
            self.server.clone(),
            self.port,
            self.flow_resume()?,
        ))
    }

    pub fn supports_udp_packet_path(&self) -> bool {
        !self
            .plugin
            .as_ref()
            .is_some_and(|plugin| plugin.supports_udp())
    }

    pub fn udp_packet_path_plan(
        &self,
    ) -> Result<ShadowsocksManagedUdpPacketPathPlan, zero_core::Error> {
        Ok(ShadowsocksManagedUdpPacketPathPlan::new(
            self.server.clone(),
            self.port,
            self.packet_path_carrier_descriptor()?,
            self.packet_path_carrier_codec()?,
            self.packet_path_datagram_source_build()?,
        ))
    }

    pub async fn open_tcp_stream<OpenSocket, OpenSocketFut, E>(
        &self,
        session: &Session,
        open_socket: OpenSocket,
    ) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>> + Send,
        E: Into<RuntimeError>,
    {
        let lease = match &self.plugin {
            Some(plugin) => plugin.acquire(true).await?,
            None => None,
        };
        let (server, port) = lease.as_ref().map_or_else(
            || (self.server.clone(), self.port),
            |lease| (lease.endpoint().ip().to_string(), lease.endpoint().port()),
        );
        let upstream = open_socket(&server, port).await.map_err(Into::into)?;
        let metered = MeteredStream::new(TcpRelayStream::from(upstream));
        let (stream, traffic) = super::tcp::establish_with_replay(
            metered,
            session,
            &self.cipher,
            &self.password,
            self.replay.clone(),
        )
        .await?;
        let stream = if let Some(lease) = lease {
            TcpRelayStream::new(super::plugin::stream::PluginStream { stream, lease })
        } else {
            stream
        };
        Ok((stream, traffic))
    }

    pub async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError> {
        if self
            .plugin
            .as_ref()
            .is_some_and(|plugin| plugin.supports_tcp())
        {
            return Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SIP003 plugins cannot wrap an existing relay stream",
            )));
        }
        super::tcp::relay_with_replay(
            stream,
            session,
            &self.cipher,
            &self.password,
            self.replay.clone(),
        )
        .await
    }

    fn flow_config(&self) -> ShadowsocksManagedUdpFlowConfig<'_> {
        let mut config = ShadowsocksManagedUdpFlowConfig::new(
            &self.tag,
            &self.server,
            self.port,
            &self.cipher,
            &self.password,
        )
        .with_replay_guard(self.replay.clone());
        config.limits = self.limits;
        config
    }
}
