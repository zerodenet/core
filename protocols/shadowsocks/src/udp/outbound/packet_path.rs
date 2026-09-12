use super::*;
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathSpec {
    pub(super) resume: ShadowsocksUdpFlowResume,
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone)]
pub struct ShadowsocksUdpPacketPathDatagramSourceBuild {
    pub(super) tag: alloc::string::String,
    pub(super) server: alloc::string::String,
    pub(super) port: u16,
    pub(super) cache_key: alloc::string::String,
    pub(super) codec: ShadowsocksDatagramCodec,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathSpec {
    pub(super) fn new(resume: ShadowsocksUdpFlowResume) -> Self {
        Self { resume }
    }

    pub fn carrier_codec(&self) -> alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>> {
        alloc::sync::Arc::new(self.resume.association.fresh_association())
    }

    pub fn datagram_source_build(
        &self,
        tag: &str,
        server: &str,
        port: u16,
    ) -> ShadowsocksUdpPacketPathDatagramSourceBuild {
        ShadowsocksUdpPacketPathDatagramSourceBuild {
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            cache_key: self.resume.flow_cache_key(),
            codec: self.resume.association.fresh_association(),
        }
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathCarrierBuild {
    pub(super) cache_key: alloc::string::String,
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathCarrierDescriptor {
    pub(super) cache_key: alloc::string::String,
    pub(super) server: alloc::string::String,
    pub(super) port: u16,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathSpec {
    pub fn carrier_build(&self) -> ShadowsocksUdpPacketPathCarrierBuild {
        ShadowsocksUdpPacketPathCarrierBuild {
            cache_key: self.resume.flow_cache_key(),
        }
    }

    pub fn carrier_descriptor(
        &self,
        server: &str,
        port: u16,
    ) -> ShadowsocksUdpPacketPathCarrierDescriptor {
        ShadowsocksUdpPacketPathCarrierDescriptor {
            cache_key: self.resume.flow_cache_key(),
            server: server.to_owned(),
            port,
        }
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathCarrierDescriptor {
    pub fn into_parts(self) -> (alloc::string::String, alloc::string::String, u16) {
        (self.cache_key, self.server, self.port)
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathDatagramSourceBuild {
    pub fn into_parts(
        self,
    ) -> (
        alloc::string::String,
        alloc::string::String,
        u16,
        alloc::string::String,
        ShadowsocksDatagramCodec,
    ) {
        (self.tag, self.server, self.port, self.cache_key, self.codec)
    }

    pub fn into_codec(self) -> ShadowsocksDatagramCodec {
        self.codec
    }

    pub fn into_shared_codec_parts(
        self,
    ) -> (
        alloc::string::String,
        alloc::string::String,
        u16,
        alloc::string::String,
        alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>>,
    ) {
        let (tag, server, port, cache_key, codec) = self.into_parts();
        (tag, server, port, cache_key, alloc::sync::Arc::new(codec))
    }
}
