//! UDP handlers registered at proxy assembly time and their neutral state.
//!
//! This layer selects and invokes registered managed-flow and upstream
//! association handlers. The reusable connection machinery lives in sibling
//! `managed`; concrete protocol state remains opaque.

#[cfg(feature = "managed-udp-runtime")]
mod forward;
#[cfg(feature = "udp-runtime")]
mod state;
#[cfg(feature = "upstream-association-runtime")]
mod upstream;

#[cfg(feature = "upstream-association-runtime")]
pub(crate) use state::ClosedRegisteredUpstreamAssociation;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use state::RegisteredUpstreamAssociationView;
#[cfg(feature = "udp-runtime")]
pub(crate) use state::{RegisteredUdpHandlers, RegisteredUdpState};
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use upstream::boxed_registered_upstream_handler;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use upstream::UpstreamAssociationHandler;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use upstream::UpstreamAssociationSend;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use upstream::UpstreamUdpHandlers;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use upstream::{
    UpstreamAssociationCloseReason, UpstreamAssociationStages, UpstreamAssociationTarget,
    UpstreamAssociationTransport,
};

#[cfg(feature = "managed-stream-runtime")]
mod logical;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use logical::LogicalConnection;
