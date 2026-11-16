mod direct;
pub(crate) use direct::DirectDatagramTarget;
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
mod leaf;
mod outbound;
mod standard_packet;

pub(crate) use leaf::PreparedUdpLeafCandidate;
pub(crate) use outbound::PreparedUdpOutbound;

#[cfg(feature = "managed-stream-runtime")]
mod standard_relay;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use standard_relay::PreparedDatagramRelay;
