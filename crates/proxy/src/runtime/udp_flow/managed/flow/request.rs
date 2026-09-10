//! Managed UDP flow request facade.
//!
//! The root stays as a facade so datagram flow inputs, stream flow inputs, and
//! shared request envelopes do not regrow into one implementation bucket.

#[cfg(feature = "managed-datagram-runtime")]
mod datagram;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
mod envelope;
#[cfg(feature = "managed-stream-runtime")]
mod stream;

#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use datagram::ManagedDatagramFlow;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use envelope::{ManagedExistingFlowForward, ManagedUdpFlowKind, ManagedUdpFlowRequest};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use stream::{
    ManagedRelayStreamCarrier, ManagedRelayStreamFlow, ManagedStreamPacketFlow,
};
