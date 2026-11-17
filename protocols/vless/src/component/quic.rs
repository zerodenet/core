//! One real per-call QUIC endpoint; native and borrowed share this actual N.
mod carrier;
pub(in crate::component) use carrier::closed_marker;
pub(crate) use carrier::Registry as CarrierRegistry;
pub use carrier::TransportCarrier;
mod connect;
mod entry;
mod factory;
pub(in crate::component) mod hysteria;
pub mod mixed;
mod options;
mod pending;
mod roles;
pub use factory::Factory;
pub use options::Options;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::contract::{
    component::*, scope::TaskRegistrar, ChannelError, Impact, Reason, Stage,
};
#[track_caller]
fn error(stage: Stage, reason: Reason) -> ChannelError {
    tracing::debug!(?stage, ?reason, location = %std::panic::Location::caller(),
        "QUIC component operation failed");
    ChannelError::new(stage, reason, Impact::Component)
}
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("validated QUIC resource dimension")
}

use zero_traits::packet_metadata::PacketMetadataArc;
