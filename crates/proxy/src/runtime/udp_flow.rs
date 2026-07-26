//! Neutral UDP flow models, persistent state, and execution mechanisms.
//!
//! Protocol-specific inbound UDP association/session handling lives under the
//! owning adapter listener modules.
//!
//! `managed` owns reusable runtime mechanisms for resumable stream/datagram
//! flows. `registered` owns the handler set assembled by `register.rs` and the
//! neutral state used to invoke those handlers. Neither module owns concrete
//! protocol parsing, framing, or crypto state.

#[cfg(feature = "udp-runtime")]
pub(crate) mod managed;
#[cfg(feature = "udp-runtime")]
pub(crate) mod outbound;
pub(crate) mod packet_path;
#[cfg(feature = "udp-runtime")]
pub(crate) mod packet_path_chain;
#[cfg(feature = "udp-runtime")]
pub(crate) mod rate_limit;
#[cfg(feature = "udp-runtime")]
pub(crate) mod registered;
#[cfg(feature = "upstream-association-runtime")]
pub(crate) mod response;
pub(crate) mod result;
#[cfg(feature = "udp-runtime")]
pub(crate) mod sessions;
#[cfg(feature = "udp-runtime")]
pub(crate) mod snapshot;
#[cfg(feature = "udp-runtime")]
pub(crate) mod state;
