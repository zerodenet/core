//! Generic UDP packet-path flow abstractions.
//!
//! The root stays as a facade so runtime flow context, carrier contracts,
//! datagram descriptors, and packet-path snapshots do not regrow into one
//! large implementation bucket.

mod carrier;
mod context;
mod datagram;
mod snapshot;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod tuple_flow;

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
#[allow(unused_imports)]
pub(crate) use carrier::{
    packet_path_carrier_descriptor, packet_path_carrier_descriptor_from_build,
    PacketPathCarrierDescriptorBuild,
};
#[cfg(feature = "upstream-association-runtime")]
#[allow(unused_imports)]
pub(crate) use carrier::{packet_path_payload_carrier, PacketPathPayloadTransport};
#[allow(unused_imports)]
pub(crate) use carrier::{PacketPathCarrier, PacketPathCarrierDescriptor};
#[cfg(feature = "udp-runtime")]
#[allow(unused_imports)]
pub(crate) use context::{ChainTask, UdpFlowContext, UdpPacketRef};
#[cfg(feature = "managed-datagram-runtime")]
#[allow(unused_imports)]
pub(crate) use datagram::{
    udp_datagram_source, udp_datagram_source_from_build, UdpDatagramSourceBuild,
};
#[allow(unused_imports)]
pub(crate) use datagram::{DatagramCodec, UdpDatagramDescriptor, UdpDatagramSource};
#[cfg(feature = "udp-runtime")]
#[allow(unused_imports)]
pub(crate) use datagram::{UdpDatagramEndpoint, UdpDatagramKey};
#[cfg(feature = "udp-runtime")]
#[allow(unused_imports)]
pub(crate) use snapshot::{PacketPathFlowBinding, PacketPathFlowSnapshot, PacketPathLookupKey};
