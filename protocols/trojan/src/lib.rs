//! Trojan protocol implementation (trojan-go spec).
//!
//! Trojan tunnels TCP/UDP over TLS with password authentication.
//! The upstream server validates the password, reads the target address,
//! and relays traffic.

#![allow(async_fn_in_trait)]

pub mod inbound;
pub mod metadata;
#[cfg(feature = "tokio")]
pub mod mux;
pub mod outbound;
mod shared;
#[cfg(feature = "runtime")]
pub mod transport;
pub mod udp;
pub mod validation;
