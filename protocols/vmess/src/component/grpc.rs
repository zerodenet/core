//! Explicit library-only per-connection gRPC ownership; no Client reuse/pool.
mod options;
pub use options::Options;
mod lifecycle;
mod reception;
pub(crate) mod registry;
mod state;
pub use lifecycle::Factory;
mod connect;
mod wire;
use std::sync::{Arc, Mutex};
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
fn error(instance: ComponentInstanceId, stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}

#[cfg(test)]
#[path = "../../tests/component/grpc_hosted.rs"]
mod tests;
