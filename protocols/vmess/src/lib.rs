#![allow(async_fn_in_trait)]

#[cfg(feature = "runtime")]
mod auth;
#[cfg(feature = "runtime")]
mod crypto;
#[cfg(feature = "runtime")]
pub mod inbound;
pub mod metadata;
#[cfg(feature = "runtime")]
pub mod mux;
#[cfg(feature = "runtime")]
pub mod outbound;
#[cfg(feature = "runtime")]
mod shared;
#[cfg(feature = "runtime")]
pub mod stream;
#[cfg(feature = "runtime")]
pub mod transport;
#[cfg(feature = "runtime")]
pub mod udp;
#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "validation")]
pub use validation::{parse_uuid, VmessCipher};
