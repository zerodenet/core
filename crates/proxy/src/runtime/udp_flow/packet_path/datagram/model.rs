use std::sync::Arc;

use zero_core::Address;

/// Datagram codec for encoding/decoding inner protocol datagrams.
pub(crate) use zero_traits::DatagramCodec;

/// Datagram source params for a relay-chain final hop over a packet path.
///
/// Produced by the owning adapter's cold packet projection. The
/// `cache_key` feeds packet-path cache identity without exposing raw config
/// parsing to the manager.
#[derive(Clone)]
pub(crate) struct UdpDatagramDescriptor {
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) cache_key: String,
}

/// Adapter-provided datagram role output for packet-path relay chains.
///
/// The descriptor is the generic chain-management surface. The codec is the
/// protocol-provided packet framing object for the selected datagram hop.
#[derive(Clone)]
pub(crate) struct UdpDatagramSource {
    pub(crate) descriptor: UdpDatagramDescriptor,
    pub(crate) codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
}

#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UdpDatagramKey {
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) cache_key: String,
}

#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UdpDatagramEndpoint {
    pub(super) server: String,
    pub(super) port: u16,
}
