mod association;
pub(in crate::runtime::udp_flow::registered) use association::UpstreamAssociationState;
#[cfg(feature = "upstream-association-runtime")]
mod tracked;

#[cfg(feature = "upstream-association-runtime")]
pub(super) use tracked::TrackedUpstreamAssociation;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use tracked::TrackedUpstreamAssociationState;
