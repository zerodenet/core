//! Post-accept TCP ingress contract and unified session pipeline.
//!
//! The `InboundProtocol` trait is the boundary between protocol-specific
//! client response/relay handling and the kernel's protocol-agnostic pipeline.
//! Carrier accept, protocol authentication, and route/session construction
//! happen before `serve_inbound()` handoff. The kernel owns connection
//! counting, rate limiting, routing, metering, and session
//! lifecycle; protocol handlers never touch those directly.

mod contract;
mod lifecycle;
mod runtime;

#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use contract::ClientResponseInboundProtocol;
pub(crate) use contract::InboundProtocol;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use contract::NoClientResponseInboundProtocol;
pub(crate) use contract::NoClientResponseStreamProtocol;
pub(crate) use contract::{MessageInboundProtocol, MessageRelayContext, MessageRelayOutcome};
#[cfg(feature = "udp-runtime")]
pub(crate) use lifecycle::apply_kernel_rate_limits_from_config;
pub(crate) use runtime::TcpIngressRuntime;
