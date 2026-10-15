use serde::{Deserialize, Serialize};

/// Shared dispatcher delivery working set, repeated per sink; never sum it.
/// Charges are serialized event bytes for live deliveries and whole journal
/// frame bytes for recovered ones. They conservatively count each sink's
/// obligation even when it shares the event allocation. This is not RSS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryBufferStatus {
    pub byte_budget: u64,
    /// Includes queued, retry/ACK-retry and in-flight event ownership.
    pub reserved_bytes: u64,
    /// One oversized event, or inherited state after lowering a reload limit,
    /// may exceed the budget. Additional admission pauses until it drains.
    pub over_budget: bool,
}
