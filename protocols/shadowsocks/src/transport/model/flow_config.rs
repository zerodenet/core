//! Existing flow config projection, without allocating any runtime owner.
use super::*;
impl<'a> ShadowsocksManagedUdpFlowConfig<'a> {
    pub(in crate::transport) fn with_replay_guard(
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

    pub fn flow_resume(&self) -> Result<ShadowsocksManagedDatagramFlowResume, zero_core::Error> {
        self.protocol_config()
            .flow_resume()
            .map(ShadowsocksManagedDatagramFlowResume::new)
    }

    pub fn packet_path_carrier_descriptor(
        &self,
    ) -> Result<ShadowsocksManagedUdpPacketPathCarrierDescriptor, zero_core::Error> {
        Ok(ShadowsocksManagedUdpPacketPathCarrierDescriptor::new(
            self.protocol_config()
                .packet_path_spec()?
                .carrier_descriptor(self.server, self.port),
        ))
    }

    pub fn packet_path_carrier_codec(
        &self,
    ) -> Result<Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>, zero_core::Error> {
        Ok(self.protocol_config().packet_path_spec()?.carrier_codec())
    }

    pub(in crate::transport) fn packet_path_spec(
        &self,
    ) -> Result<crate::udp::ShadowsocksUdpPacketPathSpec, zero_core::Error> {
        self.protocol_config().packet_path_spec()
    }

    pub fn packet_path_datagram_source_build(
        &self,
    ) -> Result<ShadowsocksManagedUdpPacketPathDatagramSourceBuild, zero_core::Error> {
        Ok(ShadowsocksManagedUdpPacketPathDatagramSourceBuild::new(
            self.protocol_config()
                .packet_path_spec()?
                .datagram_source_build(self.tag, self.server, self.port),
        ))
    }

    fn protocol_config(&self) -> crate::udp::ShadowsocksUdpFlowConfig<'a> {
        crate::udp::ShadowsocksUdpFlowConfig::new(
            self.tag,
            self.server,
            self.port,
            self.cipher,
            self.password,
        )
        .with_replay_guard(self.replay.clone())
        .with_state_limits(self.limits)
    }
}
