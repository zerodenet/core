//! One real per-call QUIC endpoint; native and borrowed share this actual N.
mod connect;
mod entry;
mod factory;
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
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("validated QUIC resource dimension")
}

use zero_traits::packet_metadata::PacketMetadataArc;
