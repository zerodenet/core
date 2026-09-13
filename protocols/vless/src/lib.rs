#![cfg_attr(not(feature = "tokio"), no_std)]
#![allow(async_fn_in_trait)]

extern crate alloc;

#[cfg(any(feature = "runtime", feature = "validation"))]
pub mod encryption;

#[cfg(feature = "runtime")]
pub mod deferred_response;
#[cfg(feature = "runtime")]
pub mod fallback;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod flow;
mod flow_name;
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
#[cfg(any(feature = "validation", feature = "runtime"))]
pub mod reality_policy;
#[cfg(any(feature = "validation", feature = "runtime"))]
pub mod reality_spider;
#[cfg(feature = "runtime")]
pub mod reverse;
#[cfg(feature = "runtime")]
mod shared;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod transport;
#[cfg(feature = "runtime")]
pub mod udp;
mod uuid;
#[cfg(any(feature = "validation", feature = "runtime"))]
pub mod validation;
#[cfg(all(feature = "runtime", feature = "reality"))]
pub mod vision;

#[cfg(feature = "runtime")]
pub use shared::VLESS_VERSION;
pub use uuid::{format_uuid, parse_uuid};

#[cfg(all(feature = "runtime", feature = "tokio"))]
mod mlkem;
