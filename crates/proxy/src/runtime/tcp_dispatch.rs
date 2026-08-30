//! TCP connection dispatch: routing pipeline and neutral relay orchestration.
//!
//! The root stays as a facade so TCP leaf dispatch and relay-chain glue can be
//! split without turning this file back into a catch-all implementation bucket.

mod candidate;
mod intent;
mod leaf;
pub(crate) mod operation;
mod outbound;
pub(crate) mod relay;

pub(crate) use intent::TcpDispatchIntent;
pub(crate) use leaf::dispatch_tcp;
pub(crate) use outbound::dispatch_tcp_outbound;
