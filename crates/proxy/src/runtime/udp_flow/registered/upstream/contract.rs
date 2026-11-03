mod model;
#[cfg(feature = "upstream-association-runtime")]
mod target;
#[cfg(feature = "upstream-association-runtime")]
mod transport;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) use model::UpstreamAssociationCloseReason;
pub(crate) use model::UpstreamAssociationSend;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use target::{DatagramAssociationTarget, UpstreamAssociationTarget};
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use transport::UpstreamAssociationTransport;
