//! Shared XUDP continuity identities. Standard Datagram routing owns execution.
mod continuity;

pub(crate) use continuity::{
    MuxUdpContinuityAttach, MuxUdpContinuityRegistry, MuxUdpContinuityScope,
    MuxUdpDetachedCancellation,
};
