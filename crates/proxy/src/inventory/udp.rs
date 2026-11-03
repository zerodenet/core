#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
mod association;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) use association::DatagramAssociationTarget;
mod datagram_relay;
mod leaf;
mod outbound;
mod packet_path;
mod relay;

pub(crate) use leaf::PreparedUdpLeafCandidate;
pub(crate) use outbound::PreparedUdpOutbound;

#[cfg(feature = "managed-stream-runtime")]
mod standard_relay;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use standard_relay::PreparedDatagramRelay;
