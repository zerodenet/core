//! Neutral datagram-carried UDP runtime glue.
//!
//! The root stays as a facade over the request model and shared event loop so
//! protocol-owned datagram responders hand off into one runtime template.

mod contract;
mod lifecycle;

pub(crate) use lifecycle::run_protocol_datagram_udp_relay;
