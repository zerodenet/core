#![cfg_attr(not(feature = "tokio"), no_std)]
#![allow(async_fn_in_trait)]

extern crate alloc;

#[cfg(feature = "runtime")]
pub mod deferred_response;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod flow;
#[cfg(feature = "runtime")]
pub mod inbound;
pub mod metadata;
#[cfg(feature = "runtime")]
pub mod mux;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod mux_crypto;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod mux_pool;
#[cfg(feature = "runtime")]
pub mod outbound;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod reality;
#[cfg(feature = "runtime")]
mod shared;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod transport;
#[cfg(feature = "runtime")]
pub mod udp;
mod uuid;
#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "runtime")]
pub use shared::VLESS_VERSION;
pub use uuid::{format_uuid, parse_uuid};
