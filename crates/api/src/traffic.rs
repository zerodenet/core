//! Observation periods over authoritative monotonic traffic counters.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TrafficScope {
    Global,
    Inbound {
        tag: String,
    },
    Outbound {
        tag: String,
    },
    Endpoint {
        endpoint_id: String,
    },
    Peer {
        endpoint_id: String,
        peer_id: String,
    },
    /// Host-wide interface facts; never attributed to an endpoint or business flow.
    HostInterface {
        name: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficPlane {
    Flow,
    Inner,
    Outer,
    Host,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficRole {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficMetric {
    BytesUp,
    BytesDown,
    RxBytes,
    TxBytes,
    RxPackets,
    TxPackets,
    DroppedPackets,
    Errors,
    RxDroppedPackets,
    TxDroppedPackets,
    RxErrors,
    TxErrors,
}

impl TrafficMetric {
    pub const ALL: [Self; 12] = [
        Self::BytesUp,
        Self::BytesDown,
        Self::RxBytes,
        Self::TxBytes,
        Self::RxPackets,
        Self::TxPackets,
        Self::DroppedPackets,
        Self::Errors,
        Self::RxDroppedPackets,
        Self::TxDroppedPackets,
        Self::RxErrors,
        Self::TxErrors,
    ];
}

/// null is unavailable; zero is an observed zero. No inferred packet counts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficCounters {
    pub bytes_up: Option<u64>,
    pub bytes_down: Option<u64>,
    pub rx_bytes: Option<u64>,
    pub tx_bytes: Option<u64>,
    pub rx_packets: Option<u64>,
    pub tx_packets: Option<u64>,
    pub dropped_packets: Option<u64>,
    pub errors: Option<u64>,
    #[serde(default)]
    pub rx_dropped_packets: Option<u64>,
    #[serde(default)]
    pub tx_dropped_packets: Option<u64>,
    #[serde(default)]
    pub rx_errors: Option<u64>,
    #[serde(default)]
    pub tx_errors: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficDropReason {
    Unspecified,
    QueueFull,
    QueueClosed,
    InvalidPacket,
    SourceRejected,
    FragmentRejected,
    PolicyRejected,
    IoFailure,
    NoRoute,
    HopLimit,
}
impl TrafficDropReason {
    pub const ALL: [Self; 10] = [
        Self::Unspecified,
        Self::QueueFull,
        Self::QueueClosed,
        Self::InvalidPacket,
        Self::SourceRejected,
        Self::FragmentRejected,
        Self::PolicyRejected,
        Self::IoFailure,
        Self::NoRoute,
        Self::HopLimit,
    ];
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficDropCounter {
    pub reason: TrafficDropReason,
    pub packets: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficPlaneSnapshot {
    pub plane: TrafficPlane,
    pub accounting_basis: String,
    pub counters: TrafficCounters,
    pub available_metrics: Vec<TrafficMetric>,
    pub resettable_metrics: Vec<TrafficMetric>,
    /// Explicit execution roles observed by this source; empty means not a role source.
    #[serde(default)]
    pub source_roles: Vec<TrafficRole>,
    /// Sparse, observed local discard reasons. Missing reasons are not observed zeros.
    /// These are reset with the containing scope; they are not additive to dropped_packets.
    #[serde(default)]
    pub drop_reasons: Vec<TrafficDropCounter>,
    #[serde(default)]
    pub drop_reasons_resettable: bool,
    #[serde(default)]
    pub drop_coverage: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficActivity {
    pub active_stream_flows: Option<u64>,
    pub active_datagram_flows: Option<u64>,
    pub active_packet_routes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficSnapshot {
    pub scope: TrafficScope,
    pub core_instance_id: String,
    pub config_revision: u64,
    pub generation: Option<u64>,
    pub stats_epoch: String,
    pub epoch_started_at_unix_ms: u64,
    pub capture_started_at_unix_ms: u64,
    /// Per-scope monotonic clock. Compare only within one instance/epoch.
    pub sampled_at_monotonic_ns: u64,
    pub sampled_at_unix_ms: u64,
    pub planes: Vec<TrafficPlaneSnapshot>,
    pub activity: TrafficActivity,
    /// Reset changes all available cumulative metrics; instantaneous state is excluded.
    pub reset_policy: String,
    /// Last authoritative provider read, distinct from query capture time.
    #[serde(default)]
    pub source_sampled_at_unix_ms: Option<u64>,
    #[serde(default)]
    pub source_sampled_at_monotonic_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrafficGetQuery {
    pub scope: TrafficScope,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrafficListQuery {
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub limit: Option<usize>,
    /// Empty selects the default inventory; host scopes require explicit opt-in.
    #[serde(default)]
    pub scopes: Vec<TrafficScope>,
    #[serde(default)]
    pub expected_core_instance_id: Option<String>,
    #[serde(default)]
    pub expected_config_revision: Option<u64>,
    #[serde(default)]
    pub expected_registry_revision: Option<u64>,
    /// Preserve legacy default pages. New clients opt in after capability discovery.
    #[serde(default, skip_serializing_if = "is_false")]
    pub include_host_interfaces: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficListSnapshot {
    pub core_instance_id: String,
    pub config_revision: u64,
    pub registry_revision: u64,
    pub sampled_at_unix_ms: u64,
    pub scopes: Vec<TrafficSnapshot>,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsResetTarget {
    pub scope: TrafficScope,
    pub expected_stats_epoch: String,
    #[serde(default)]
    pub expected_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsResetCommand {
    pub expected_core_instance_id: String,
    /// Explicit identities; global alone never cascades to children.
    pub targets: Vec<StatsResetTarget>,
    #[serde(default)]
    pub operation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsResetSnapshot {
    pub operation_id: String,
    pub core_instance_id: String,
    pub snapshots: Vec<TrafficSnapshot>,
}

/// Additive discovery; individual snapshots remain authoritative for availability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficStatisticsCapability {
    pub contract_version: u32,
    pub queries: Vec<String>,
    pub reset_command: String,
    pub reset_permission: crate::Permission,
    pub resettable_scopes: Vec<String>,
    pub reset_policy: String,
    pub events: Vec<String>,
    pub automatic_sampling: bool,
    pub sample_interval_ms: u64,
    pub sample_page_size: usize,
    pub maximum_page_size: usize,
    pub maximum_reset_targets: usize,
    pub history: String,
    pub retry_policy: String,
    pub capture_consistency: String,
    pub sampling_strategy: String,
    #[serde(default)]
    pub local_drop_reasons: bool,
    #[serde(default)]
    pub host_interface_sampling: bool,
}
impl Default for TrafficStatisticsCapability {
    fn default() -> Self {
        Self {
            local_drop_reasons: true,
            host_interface_sampling: false,
            sampling_strategy: "round_robin_bounded_pages".into(),
            contract_version: 1,
            queries: vec!["traffic_stat".into(), "traffic_stats".into()],
            reset_command: "stats.reset".into(),
            reset_permission: crate::Permission::Admin,
            resettable_scopes: ["global", "inbound", "outbound", "endpoint", "peer"]
                .map(str::to_owned)
                .into(),
            reset_policy: "all_available_cumulative_no_cascade".into(),
            events: vec![
                crate::event_type::STATS_SCOPES_SAMPLED.into(),
                crate::event_type::STATS_RESET.into(),
            ],
            automatic_sampling: false,
            sample_interval_ms: 1000,
            sample_page_size: 64,
            maximum_page_size: 256,
            maximum_reset_targets: 256,
            history: "bounded_event_replay_no_time_series".into(),
            retry_policy: "epoch_compare_and_swap_conflict_on_replay".into(),
            capture_consistency: "atomic_per_counter_capture_window_serialized_period_boundary"
                .into(),
        }
    }
}
