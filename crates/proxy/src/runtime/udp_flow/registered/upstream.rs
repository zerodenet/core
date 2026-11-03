mod contract;
mod native;
#[cfg(feature = "udp-runtime")]
mod runtime;
#[cfg(feature = "udp-runtime")]
mod state;

pub(crate) use contract::UpstreamAssociationSend;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use contract::{
    DatagramAssociationTarget, UpstreamAssociationCloseReason, UpstreamAssociationTarget,
    UpstreamAssociationTransport,
};
#[cfg(all(test, feature = "upstream-association-runtime"))]
pub(crate) use runtime::UpstreamAssociationRuntime;
#[cfg(all(test, feature = "upstream-association-runtime"))]
pub(crate) use state::TrackedUpstreamAssociationState;
#[cfg(feature = "udp-runtime")]
pub(super) use state::UpstreamAssociationState;

#[cfg(all(test, feature = "upstream-association-runtime"))]
mod tests;
