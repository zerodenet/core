//! TCP ingress lifecycle facade.
//!
//! The root stays as a facade so session setup, relay outcome handling, and
//! rate-limit projection do not regrow into one implementation bucket.

mod rate_limit;
pub(crate) mod result;
pub(super) mod serve;

pub(crate) use rate_limit::apply_kernel_rate_limits_from_config;
pub(crate) use serve::serve_inbound;
