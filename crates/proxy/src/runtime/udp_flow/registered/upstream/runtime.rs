#[cfg(feature = "upstream-association-runtime")]
pub(super) mod association;
mod mismatch;

#[cfg(all(test, feature = "upstream-association-runtime"))]
pub(crate) use association::UpstreamAssociationRuntime;
pub(crate) use mismatch::upstream_flow_mismatch;
