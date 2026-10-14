use serde::{Deserialize, Serialize};

/// Shared Connector source admission, repeated in each sink's status for
/// convenience. Do not sum across sinks. These are delivery gauges, not
/// business traffic. High-water and oversized counters reset on replacement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceBufferStatus {
    /// Channel slots; reservations also include the sender and selected input.
    pub capacity_events: u64,
    pub byte_budget: u64,
    /// Current subscription's live bridge events retained for admission, not
    /// replay/handoff payloads or delivery workset copies. Gauges are read
    /// independently rather than as a transaction snapshot.
    pub reserved_events: u64,
    /// Charged serialized-byte budget; an oversized event charges the entire
    /// budget. This is not resident heap memory.
    pub reserved_bytes: u64,
    pub high_water_events: u64,
    pub high_water_bytes: u64,
    /// One already-owned event waiting for budget admission, serialized bytes.
    pub waiting_event_bytes: u64,
    pub oversized_events: u64,
    pub paused: bool,
}
