//! Exact additional buffers of the same declared Packet native original.
//! Counts and pure byte lengths issue no IO/task/close permission.
use std::num::NonZeroUsize;
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PacketSupplementalMemory {
    /// Existing original receive workers, prepaid before the descriptor binds.
    pub receive_workspaces: Vec<NonZeroUsize>,
    /// One serialized wire's separate pending transport working-copy buffer.
    pub transmit_copy: Option<NonZeroUsize>,
    /// Separate owned per-datagram Domain backing, never borrowed from the
    /// payload allocation or an unrelated codec's decoded target workspace.
    pub transmit_target: Option<NonZeroUsize>,
}
