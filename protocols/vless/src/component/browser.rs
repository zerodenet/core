//! Browser-assisted carriers delegate physical Source ownership to `zero_transport::browser_dialer::component`.
pub(in crate::component) mod factory;
mod options;
mod roles;
pub use factory::Factory;
pub use options::Options;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    component::*,
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    ChannelError, CloseReason, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::PacketMetadataArc;

fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
