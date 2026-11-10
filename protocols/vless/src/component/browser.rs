//! Browser-assisted carriers use the original Host Source, never a second listener.
mod acquire;
mod delivery;
mod diagnostics;
mod entry;
mod factory;
mod http;
mod options;
mod pending;
mod roles;
mod table;
pub use factory::Factory;
pub use options::Options;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::contract::{
    component::*,
    lifecycle::{CloseReport, Unfinished},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("validated Browser dimension")
}
