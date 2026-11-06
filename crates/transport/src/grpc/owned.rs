//! Controlled library ownership of the same pool, never a replacement pool.
mod options;
pub(super) use options::Recipe;
pub use options::{NativePoolOptions, NativePoolSettings};
mod reception;
mod state;
pub(super) use state::{Managed, Ownership};
#[cfg(feature = "tls")]
mod ech;
mod lifecycle;
pub use lifecycle::NativePoolFactory;
mod client;
pub(super) use client::NativeClient;
mod budget;
mod carrier;
mod connect;
mod epoch;
mod totals;
pub use totals::{PoolBudgets, PoolTotals, TotalsFactory, TotalsOptions, TotalsProposal};
mod logical;
mod opening;
mod pending;
mod setup;
mod stream;
pub use stream::NativeGrpcStream;
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{CancellationToken, ChannelError, Impact, Reason, Stage};

/// One invocation's real deadline and cancellation; never retained by the pool.
pub struct Demand {
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
    pub binding: zero_platform_tokio::contract::network::UpstreamBinding,
}
fn error(instance: ComponentInstanceId, stage: Stage, reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error
}

#[cfg(test)]
#[path = "../../tests/grpc/owned/mod.rs"]
mod tests;
