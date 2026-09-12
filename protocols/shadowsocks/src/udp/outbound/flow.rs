use super::*;
#[cfg(feature = "crypto")]
#[derive(Debug, Clone)]
pub struct ShadowsocksUdpSocketFlowSpec {
    pub(super) cache_key: alloc::string::String,
    pub(super) codec: ShadowsocksDatagramCodec,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpSocketFlowSpec {
    pub fn into_cache_key(self) -> alloc::string::String {
        self.cache_key
    }

    pub fn into_codec(self) -> ShadowsocksDatagramCodec {
        self.codec
    }
}

#[cfg(feature = "crypto")]
#[derive(Clone)]
pub struct ShadowsocksUdpFlowResume {
    pub(super) cache_key: alloc::string::String,
    pub(super) cipher: crate::shared::CipherKind,
    pub(super) password: alloc::vec::Vec<u8>,
    pub(super) association: ShadowsocksDatagramCodec,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpFlowResume {
    pub(crate) fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.cache_key.push_str(&format!(":limits:{limits:?}"));
        self.association = self.association.with_state_limits(limits);
        self
    }

    pub(crate) fn with_carrier_identity(mut self, identity: &str) -> Self {
        self.cache_key.push(':');
        self.cache_key.push_str(identity);
        self
    }

    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.cache_key
            .push_str(&format!(":replay:{:?}", replay.policy()));
        self.association = self.association.with_replay_guard(replay);
        self
    }

    pub fn new(
        cache_key: alloc::string::String,
        cipher: crate::shared::CipherKind,
        password: &[u8],
    ) -> Self {
        Self {
            cache_key,
            cipher,
            password: password.to_vec(),
            association: ShadowsocksDatagramCodec::new(cipher, password),
        }
    }

    pub fn from_config(
        tag: &str,
        server: &str,
        port: u16,
        cipher: &str,
        password: &str,
    ) -> Result<Self, Error> {
        let cipher_kind = parse_udp_cipher(cipher)?;
        Ok(Self::new(
            udp_cache_key(tag, server, port, cipher, password),
            cipher_kind,
            password.as_bytes(),
        ))
    }

    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    pub fn leaf_cache_key(&self) -> ShadowsocksUdpLeafKey {
        ShadowsocksUdpLeafKey {
            cache_key: self.cache_key.clone(),
        }
    }

    pub fn flow_cache_key(&self) -> alloc::string::String {
        self.cache_key.clone()
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        self.association.fresh_association()
    }

    pub fn socket_flow_codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        self.codec()
    }

    pub fn managed_socket_flow(&self) -> ShadowsocksUdpSocketFlowSpec {
        ShadowsocksUdpSocketFlowSpec {
            cache_key: self.flow_cache_key(),
            codec: self.association.fresh_association(),
        }
    }

    pub fn into_managed_socket_flow_codec(self) -> ShadowsocksDatagramCodec {
        self.managed_socket_flow().into_codec()
    }

    pub fn into_shared_managed_socket_flow_codec(
        self,
    ) -> alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>> {
        alloc::sync::Arc::new(self.into_managed_socket_flow_codec())
    }

    pub(super) fn encode_flow_packet(
        &self,
        packet: &ShadowsocksUdpFlowPacket,
    ) -> Result<alloc::vec::Vec<u8>, Error> {
        self.association
            .encode(packet.target(), packet.port(), packet.payload())
    }

    pub fn decode_flow_packet(&self, data: &[u8]) -> Option<ShadowsocksUdpFlowPacket> {
        let (target, port, payload) = self.association.decode(data)?;
        Some(ShadowsocksUdpFlowPacket {
            target,
            port,
            payload,
        })
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowsocksUdpLeafKey {
    pub(super) cache_key: alloc::string::String,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpLeafKey {
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }
}

impl PartialEq for ShadowsocksUdpFlowResume {
    fn eq(&self, other: &Self) -> bool {
        self.cache_key == other.cache_key
            && self.cipher == other.cipher
            && self.password == other.password
    }
}
impl Eq for ShadowsocksUdpFlowResume {}

impl core::fmt::Debug for ShadowsocksUdpFlowResume {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksUdpFlowResume")
            .field("cipher", &self.cipher)
            .field("cache_key", &self.cache_key)
            .finish_non_exhaustive()
    }
}
