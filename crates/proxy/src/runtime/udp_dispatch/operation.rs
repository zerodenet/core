//! Prepared UDP operation contracts plus focused executor modules.
//!
//! The root stays as a facade so direct, managed-datagram, registered, and
//! managed-stream-packet execution do not regrow into one large bucket.

mod contract;
mod direct;
#[cfg(feature = "managed-datagram-runtime")]
mod managed_datagram;
#[cfg(feature = "upstream-association-runtime")]
mod registered;
#[cfg(feature = "managed-stream-runtime")]
mod stream_packet;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod transport;

pub(crate) use contract::PreparedUdpFlowOperation;
pub(crate) use direct::DirectUdpFlowOperation;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use managed_datagram::{ManagedDatagramStartPlan, ManagedDatagramUdpOperation};
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use registered::RegisteredAssociationUdpOperation;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use stream_packet::{
    ManagedStreamPacketBridgePlan, ManagedStreamPacketUdpOperation,
    PreparedManagedStreamPacketOperation,
};

#[cfg(feature = "managed-stream-runtime")]
mod logical;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use logical::LogicalUdpOperation;
