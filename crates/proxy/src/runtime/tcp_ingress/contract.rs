//! TCP ingress contract facade.
//!
//! The root stays as a facade so flow accounting, protocol trait defaults, and
//! actual stream ownership do not regrow into one implementation bucket.

mod accounting;
mod activity;
mod no_response;
mod protocol;

pub(crate) use activity::TcpActivityStream;
pub(crate) use activity::TcpRelayActivity;
pub(crate) use no_response::NoClientResponseStreamProtocol;
pub(crate) use protocol::InboundProtocol;
