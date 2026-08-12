//! TCP ingress lifecycle facade.
//!
//! The root stays as a facade so session setup, relay outcome handling, and
//! rate-limit projection do not regrow into one implementation bucket.

mod rate_limit;
pub(crate) mod result;
mod serve;

pub(crate) use rate_limit::apply_kernel_rate_limits_from_config;
pub(crate) use serve::serve_inbound;
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) use serve::serve_inbound_with_client_response;
