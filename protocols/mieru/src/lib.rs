// Mieru protocol — lib.rs
//
// Implements the mieru proxy protocol (https://github.com/enfein/mieru).
// XChaCha20-Poly1305 AEAD, time-based key derivation, session lifecycle,
// TCP + UDP transport with random padding anti-detection.

#![cfg_attr(not(feature = "crypto"), no_std)]
#![allow(async_fn_in_trait)]

extern crate alloc;

pub use mieru_config as config;
#[cfg(feature = "crypto")]
pub mod traffic_pattern;

pub mod metadata;
pub mod protocol;
#[cfg(feature = "crypto")]
pub mod segment;
#[cfg(feature = "crypto")]
pub mod session;
#[cfg(feature = "runtime")]
pub mod transport;
pub mod udp;

#[cfg(feature = "crypto")]
pub mod crypto;

#[cfg(feature = "crypto")]
pub mod inbound;
#[cfg(feature = "crypto")]
mod outbound;
#[cfg(feature = "crypto")]
mod tunnel;

#[cfg(feature = "crypto")]
pub use outbound::{
    establish_tcp_tunnel, tcp_outbound_profile_from_config, MieruOutbound, MieruTcpOutboundProfile,
    MieruTcpStream, MieruTcpTunnelTarget,
};
pub use protocol::MieruProtocol;

#[cfg(feature = "crypto")]
pub mod client;

#[cfg(feature = "crypto")]
pub mod packet;
