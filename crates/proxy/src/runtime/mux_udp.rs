//! Mux UDP sub-stream facade.
//!
//! The root stays as a facade so packet-session handler adaptation, relay
//! execution, and task entrypoints do not collapse back into one
//! implementation bucket.

mod continuity;
mod handler;
mod relay;
mod task;

pub(crate) use continuity::{
    MuxUdpContinuityAttach, MuxUdpContinuityRegistry, MuxUdpContinuityScope,
    MuxUdpDetachedCancellation,
};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use task::run_protocol_mux_udp_task;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use task::run_protocol_mux_udp_task_with_accept_log;
