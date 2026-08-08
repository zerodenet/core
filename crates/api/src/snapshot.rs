//! Strongly-typed snapshot definitions for control plane queries.
//!
//! These types live in `zero-api` so that all consumers (GUI, controllers, CLI, FFI)
//! can depend on the same contract without pulling in engine internals.
//!
//! **Forward compatibility rules:**
//!
//! - All structs derive `Default` or carry `#[serde(default)]` on every field
//!   so that **old consumers can deserialize responses from newer servers**
//!   (new fields are silently filled with defaults).
//! - No `#[serde(deny_unknown_fields)]` — old servers accept requests from
//!   newer clients that may send new fields.
//! - Enum variants are only ever **added**, never removed or renamed.

use serde::{Deserialize, Serialize};

// ── Top-level snapshots ─────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    #[serde(default)]
    pub config_revision: u64,
    #[serde(default)]
    pub mode: ModeSnapshot,
    #[serde(default)]
    pub rule_count: usize,
    #[serde(default)]
    pub listeners: Vec<ListenerSnapshot>,
    #[serde(default)]
    pub outbounds: Vec<OutboundTargetSnapshot>,
    #[serde(default)]
    pub outbound_groups: Vec<PolicySnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    #[serde(default)]
    pub core_instance_id: String,
    #[serde(default)]
    pub config_revision: u64,
    #[serde(default)]
    pub stats: StatsSnapshot,
    #[serde(default)]
    pub udp_upstream_idle_timeout_seconds: u64,
    #[serde(default)]
    pub udp_enabled: bool,
    #[serde(default)]
    pub log_level: String,
    #[serde(default)]
    pub log_files: Vec<String>,
    /// OS process ID of the zero daemon.
    #[serde(default)]
    pub pid: u32,
    /// Source path of the running configuration file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
    /// Started at timestamp (UNIX epoch milliseconds).
    #[serde(default)]
    pub started_at_unix_ms: u64,
    #[serde(default)]
    pub active_sessions: Vec<FlowSnapshot>,
    #[serde(default)]
    pub recent_completed_sessions: Vec<CompletedFlowSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusSnapshot {
    #[serde(default)]
    pub config: ConfigSnapshot,
    #[serde(default)]
    pub runtime: RuntimeSnapshot,
}

// ── Config sub-types ────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeSnapshot {
    #[serde(default)]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerSnapshot {
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub listen_address: String,
    #[serde(default)]
    pub listen_port: u16,
    #[serde(default)]
    pub udp_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundTargetSnapshot {
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default)]
    pub udp_enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySnapshot {
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub outbounds: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_unix_ms: Option<u64>,
    #[serde(default)]
    pub effective_chains: Vec<Vec<String>>,
    #[serde(default)]
    pub url_test_members: Vec<PolicyMemberSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_test_selection: Option<UrlTestSelectionSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlTestSelectionSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_selected: Option<String>,
    #[serde(default)]
    pub selected: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub best_candidate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub best_latency_ms: Option<u64>,
    #[serde(default)]
    pub tolerance_ms: u64,
    #[serde(default)]
    pub switched: bool,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyMemberSnapshot {
    #[serde(default)]
    pub member_tag: String,
    #[serde(default)]
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub effective_chains: Vec<Vec<String>>,
}

// ── Flow sub-types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowSnapshot {
    /// Canonical record shared with flow lifecycle events. Legacy flattened
    /// fields remain during the compatibility migration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<Box<crate::FlowRecord>>,
    #[serde(default)]
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_tag: Option<String>,
    #[serde(default)]
    pub target: AddressSnapshot,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthSnapshot>,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub started_at_unix_ms: u64,
    #[serde(default)]
    pub last_activity_at_unix_ms: u64,
    #[serde(default)]
    pub bytes_up: u64,
    #[serde(default)]
    pub bytes_down: u64,
    #[serde(default)]
    pub inbound_rx_bytes: u64,
    #[serde(default)]
    pub inbound_tx_bytes: u64,
    #[serde(default)]
    pub outbound_rx_bytes: u64,
    #[serde(default)]
    pub outbound_tx_bytes: u64,
    #[serde(default)]
    pub throughput_up_bps: u64,
    #[serde(default)]
    pub throughput_down_bps: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedFlowSnapshot {
    /// Canonical completed record shared with `flow.completed` events. Legacy
    /// flattened fields remain during the compatibility migration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<Box<crate::FlowRecord>>,
    #[serde(default)]
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_tag: Option<String>,
    #[serde(default)]
    pub target: AddressSnapshot,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthSnapshot>,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub started_at_unix_ms: u64,
    #[serde(default)]
    pub last_activity_at_unix_ms: u64,
    #[serde(default)]
    pub finished_at_unix_ms: u64,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub bytes_up: u64,
    #[serde(default)]
    pub bytes_down: u64,
    #[serde(default)]
    pub inbound_rx_bytes: u64,
    #[serde(default)]
    pub inbound_tx_bytes: u64,
    #[serde(default)]
    pub outbound_rx_bytes: u64,
    #[serde(default)]
    pub outbound_tx_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    #[serde(default)]
    pub outcome: String,
    /// Why the flow ended (standard close reason). Omitted when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddressSnapshot {
    #[serde(default)]
    pub family: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthSnapshot {
    #[serde(default)]
    pub scheme: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_key: Option<String>,
}

// ── Stats ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StatsSnapshot {
    #[serde(default)]
    pub total_started: u64,
    #[serde(default)]
    pub active_sessions: u64,
    #[serde(default)]
    pub completed_sessions: u64,
    #[serde(default)]
    pub failed_sessions: u64,
    #[serde(default)]
    pub blocked_sessions: u64,
    #[serde(default)]
    pub direct_sessions: u64,
    #[serde(default)]
    pub chained_sessions: u64,
    /// Monotonic user-direction upload bytes observed by active and completed
    /// flows in this engine instance.
    #[serde(default)]
    pub bytes_up: u64,
    /// Monotonic user-direction download bytes observed by active and completed
    /// flows in this engine instance.
    #[serde(default)]
    pub bytes_down: u64,
    /// Completed-flow attribution by final outbound. Active bytes are exposed
    /// by the global counters and flow snapshots until their outbound result is
    /// final.
    #[serde(default)]
    pub per_outbound: Vec<(String, OutboundTrafficStats)>,
    #[serde(default)]
    pub udp_upstream: UdpUpstreamStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OutboundTrafficStats {
    #[serde(default)]
    pub flows: u64,
    #[serde(default)]
    pub bytes_up: u64,
    #[serde(default)]
    pub bytes_down: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct UdpUpstreamStats {
    #[serde(default)]
    pub active_associations: u64,
    #[serde(default)]
    pub created_associations: u64,
    #[serde(default)]
    pub reused_associations: u64,
    #[serde(default)]
    pub closed_associations: u64,
    #[serde(default)]
    pub idle_timeouts: u64,
    #[serde(default)]
    pub dropped_associations: u64,
    #[serde(default)]
    pub failed_association_attempts: u64,
    #[serde(default)]
    pub send_failures: u64,
    #[serde(default)]
    pub recv_failures: u64,
    #[serde(default)]
    pub packets_sent: u64,
    #[serde(default)]
    pub packets_received: u64,
}
