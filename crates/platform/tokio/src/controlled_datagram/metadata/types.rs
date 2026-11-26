use std::net::{IpAddr, SocketAddr};
/// The actual ECN bits, with no protocol-specific representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatagramEcn {
    Ect0,
    Ect1,
    Ce,
}
impl DatagramEcn {
    pub(super) fn as_bits(self) -> u8 {
        match self {
            Self::Ect0 => 0b10,
            Self::Ect1 => 0b01,
            Self::Ce => 0b11,
        }
    }
    pub(super) fn from_bits(bits: u8) -> Option<Self> {
        Some(match bits & 0b11 {
            0b10 => Self::Ect0,
            0b01 => Self::Ect1,
            0b11 => Self::Ce,
            _ => return None,
        })
    }
}
/// Borrowed until the actual native call returns. No payload queue or copy.
#[derive(Clone, Copy, Debug)]
pub struct MetadataTransmit<'a> {
    pub destination: SocketAddr,
    pub contents: &'a [u8],
    pub source_ip: Option<IpAddr>,
    pub ecn: Option<DatagramEcn>,
    pub segment_size: Option<usize>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataCapabilities {
    pub transmit_segments: usize,
    pub receive_segments: usize,
    pub batch_size: usize,
    pub may_fragment: bool,
}
/// One receive buffer can contain multiple datagrams at `stride` boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataReceive {
    pub source: SocketAddr,
    pub destination_ip: Option<IpAddr>,
    pub len: usize,
    pub stride: usize,
    pub ecn: Option<DatagramEcn>,
}
impl Default for MetadataReceive {
    fn default() -> Self {
        Self {
            source: SocketAddr::from(([0, 0, 0, 0], 0)),
            destination_ip: None,
            len: 0,
            stride: 0,
            ecn: None,
        }
    }
}
/// A local syscall fact, never an acknowledgment from the remote peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataSendOutcome {
    pub bytes: usize,
    pub datagrams: usize,
    /// If false, only this prefix was submitted. Do not replay the whole input.
    pub complete: bool,
}
impl MetadataSendOutcome {
    pub(super) fn from_bytes(transmit: &MetadataTransmit<'_>, bytes: usize) -> Self {
        let stride = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
        Self {
            bytes,
            datagrams: if transmit.contents.is_empty() {
                1
            } else {
                bytes.div_ceil(stride)
            },
            complete: bytes == transmit.contents.len(),
        }
    }
}

/// Distinguishes a host/admission rejection from an actual native send error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataSendErrorOrigin {
    /// No native send was authorized or attempted by this call.
    Local,
    /// The original socket readiness/native send operation returned this error.
    Native,
}
/// Original send failure and the boundary that produced it.
#[derive(Debug)]
pub struct MetadataSendFailure {
    pub origin: MetadataSendErrorOrigin,
    pub error: std::io::Error,
}
impl MetadataSendFailure {
    pub(super) fn local(error: std::io::Error) -> Self {
        Self {
            origin: MetadataSendErrorOrigin::Local,
            error,
        }
    }
    pub(super) fn native(error: std::io::Error) -> Self {
        Self {
            origin: MetadataSendErrorOrigin::Native,
            error,
        }
    }
}
impl std::fmt::Display for MetadataSendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}
impl std::error::Error for MetadataSendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
