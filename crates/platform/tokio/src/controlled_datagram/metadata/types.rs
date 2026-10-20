use std::net::{IpAddr, SocketAddr};
/// The actual ECN bits, with no protocol-specific representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatagramEcn {
    Ect0,
    Ect1,
    Ce,
}
impl DatagramEcn {
    pub(super) fn native(self) -> quinn_udp::EcnCodepoint {
        match self {
            Self::Ect0 => quinn_udp::EcnCodepoint::Ect0,
            Self::Ect1 => quinn_udp::EcnCodepoint::Ect1,
            Self::Ce => quinn_udp::EcnCodepoint::Ce,
        }
    }
    fn from_native(ecn: quinn_udp::EcnCodepoint) -> Self {
        match ecn {
            quinn_udp::EcnCodepoint::Ect0 => Self::Ect0,
            quinn_udp::EcnCodepoint::Ect1 => Self::Ect1,
            quinn_udp::EcnCodepoint::Ce => Self::Ce,
        }
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
impl MetadataReceive {
    pub(super) fn from_native(meta: quinn_udp::RecvMeta) -> Self {
        Self {
            source: meta.addr,
            destination_ip: meta.dst_ip,
            len: meta.len,
            stride: meta.stride,
            ecn: meta.ecn.map(DatagramEcn::from_native),
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
