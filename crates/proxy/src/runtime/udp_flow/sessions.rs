//! UDP session-flow facade plus focused model, index, and lifecycle helpers.
//!
//! The root stays as a facade so keying, lookup indexes, and completion
//! bookkeeping do not regrow into one large implementation bucket.

mod index;
mod lifecycle;
mod model;
#[cfg(test)]
mod tests;

pub(crate) use model::{CompletedUdpFlow, UdpSessionFlows};
