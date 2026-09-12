use super::*;
#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksUdpFlowConfig<'a> {
    pub(super) limits: crate::validation::StateLimits,
    pub(super) tag: &'a str,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) cipher: &'a str,
    pub(super) password: &'a str,
    pub(super) replay: crate::shared::legacy_replay::LegacyReplay,
}

#[cfg(feature = "crypto")]
impl<'a> ShadowsocksUdpFlowConfig<'a> {
    pub(crate) fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        self
    }
    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.replay = replay;
        self
    }

    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        cipher: &'a str,
        password: &'a str,
    ) -> Self {
        Self {
            limits: Default::default(),
            tag,
            server,
            port,
            cipher,
            password,
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
        }
    }

    pub fn flow_resume(&self) -> Result<ShadowsocksUdpFlowResume, Error> {
        ShadowsocksUdpFlowResume::from_config(
            self.tag,
            self.server,
            self.port,
            self.cipher,
            self.password,
        )
        .map(|resume| {
            resume
                .with_replay_guard(self.replay.clone())
                .with_state_limits(self.limits)
        })
    }

    pub fn packet_path_spec(&self) -> Result<ShadowsocksUdpPacketPathSpec, Error> {
        Ok(ShadowsocksUdpPacketPathSpec::new(self.flow_resume()?))
    }
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_spec_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathSpec, Error> {
    ShadowsocksUdpFlowConfig::new(tag, server, port, cipher, password).packet_path_spec()
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_carrier_descriptor_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathCarrierDescriptor, Error> {
    Ok(
        udp_packet_path_spec_from_config(tag, server, port, cipher, password)?
            .carrier_descriptor(server, port),
    )
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_carrier_codec_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>>, Error> {
    Ok(udp_packet_path_spec_from_config(tag, server, port, cipher, password)?.carrier_codec())
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_datagram_source_build_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathDatagramSourceBuild, Error> {
    Ok(
        udp_packet_path_spec_from_config(tag, server, port, cipher, password)?
            .datagram_source_build(tag, server, port),
    )
}

#[cfg(feature = "crypto")]
pub fn udp_flow_resume_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpFlowResume, Error> {
    ShadowsocksUdpFlowConfig::new(tag, server, port, cipher, password).flow_resume()
}

#[cfg(feature = "crypto")]
pub fn managed_socket_flow_from_resume(
    resume: &ShadowsocksUdpFlowResume,
) -> ShadowsocksUdpSocketFlowSpec {
    resume.managed_socket_flow()
}

impl core::fmt::Debug for ShadowsocksUdpFlowConfig<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksUdpFlowConfig")
            .field("cipher", &self.cipher)
            .field("server", &self.server)
            .field("port", &self.port)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
