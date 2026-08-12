//! TCP ingress contract facade.
//!
//! The root stays as a facade so flow accounting, protocol trait defaults, and
//! client-response wrappers do not regrow into one implementation bucket.

mod accounting;
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
mod client_response;
mod message;
mod no_response;
mod protocol;

pub(crate) use accounting::{record_tcp_download, record_tcp_upload};
#[cfg(any(
    feature = "managed-stream-runtime",
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime"
))]
pub(crate) use client_response::ClientResponseInboundProtocol;
pub(crate) use message::{MessageInboundProtocol, MessageRelayContext, MessageRelayOutcome};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use no_response::NoClientResponseInboundProtocol;
pub(crate) use no_response::NoClientResponseStreamProtocol;
pub(crate) use protocol::InboundProtocol;
