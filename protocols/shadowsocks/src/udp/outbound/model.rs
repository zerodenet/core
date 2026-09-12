use super::*;
/// One plaintext UDP payload to encode into a Shadowsocks UDP datagram.
#[cfg(feature = "crypto")]
#[derive(Clone, Copy)]
pub struct ShadowsocksUdpPacketTarget<'a> {
    pub target: &'a Address,
    pub port: u16,
    pub payload: &'a [u8],
    pub cipher: crate::shared::CipherKind,
    pub password: &'a [u8],
}

/// Decryption context for a Shadowsocks UDP datagram received from upstream.
#[cfg(feature = "crypto")]
#[derive(Clone, Copy)]
pub struct ShadowsocksUdpDecodeContext<'a> {
    pub cipher: crate::shared::CipherKind,
    pub password: &'a [u8],
}

/// One decoded Shadowsocks UDP datagram.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacket {
    pub(super) target: Address,
    pub(super) port: u16,
    pub(super) payload: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpFlowPacket {
    pub(super) target: Address,
    pub(super) port: u16,
    pub(super) payload: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpFlowPacket {
    pub fn from_parts(target: &Address, port: u16, payload: &[u8]) -> Self {
        Self {
            target: target.clone(),
            port,
            payload: payload.to_vec(),
        }
    }

    pub fn encode_with(&self, resume: &ShadowsocksUdpFlowResume) -> Result<Vec<u8>, Error> {
        resume.encode_flow_packet(self)
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

impl core::fmt::Debug for ShadowsocksUdpPacketTarget<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksUdpPacketTarget")
            .field("cipher", &self.cipher)
            .field("target", &self.target)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for ShadowsocksUdpDecodeContext<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksUdpDecodeContext")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}
