//! Datagram UDP relay lifecycle facade.
//!
//! The root stays as a facade so relay orchestration, inbound read dispatch,
//! response handling, and feature-specific upstream polling do not regrow into
//! one implementation bucket.

mod read;
mod relay;
mod response;
#[cfg(test)]
mod tests;
#[cfg(feature = "upstream-association-runtime")]
mod with_upstream;
mod without_upstream;

pub(crate) use relay::run_protocol_datagram_udp_relay;
