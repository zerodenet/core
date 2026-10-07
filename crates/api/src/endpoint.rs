//! Controller-neutral network resource observation and management contracts.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::ErrorDetail;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointDirections {
    #[serde(default)]
    pub inbound: bool,
    #[serde(default)]
    pub outbound: bool,
}

impl EndpointDirections {
    pub const fn outbound_only() -> Self {
        Self {
            inbound: false,
            outbound: true,
        }
    }

    pub const fn permits(self, requested: Self) -> bool {
        (!requested.inbound || self.inbound) && (!requested.outbound || self.outbound)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointCapabilities {
    #[serde(default)]
    pub directions: EndpointDirections,
    #[serde(default)]
    pub packet: bool,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub datagram: bool,
    #[serde(default)]
    pub derived_stream: bool,
    #[serde(default)]
    pub derived_datagram: bool,
    /// The resource can authenticate and learn a peer address without a
    /// configured initial address. Requires an existing listening binding.
    #[serde(default)]
    pub peer_address_learning: bool,
    #[serde(default)]
    pub operations: Vec<String>,
    #[serde(default)]
    pub operation_capabilities: BTreeMap<String, EndpointOperationCapability>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointOperationCapability {
    #[serde(default)]
    pub persistence: Vec<EndpointPersistence>,
    #[serde(default)]
    pub preconditions: Vec<String>,
    /// Supported contractions while running, independent of the current intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live_direction_contraction: Option<EndpointDirections>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointConfigurationOrigin {
    #[default]
    Unknown,
    Canonical,
    Legacy,
}

/// Base configuration facts, before any runtime intent override.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointConfiguration {
    pub origin: EndpointConfigurationOrigin,
    pub enabled: bool,
    pub directions: EndpointDirections,
    pub source_file: EndpointSourceFileCapability,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointSourceFileCapability {
    /// Structural support; caller permissions and filesystem state are separate.
    pub available: bool,
    /// Last actual write result, not an assertion about future filesystem access.
    pub writable: Option<bool>,
    pub writable_observed_at_unix_ms: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRuntimeState {
    #[default]
    Stopped,
    Starting,
    Running,
    Stopping,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointHealthState {
    #[default]
    Unknown,
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointStateSource {
    #[default]
    Config,
    RuntimeOverride,
}

/// Missing counters mean unavailable, rather than an observed zero.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointCounters {
    pub inner_rx_bytes: Option<u64>,
    pub inner_tx_bytes: Option<u64>,
    pub inner_rx_packets: Option<u64>,
    pub inner_tx_packets: Option<u64>,
    pub outer_rx_bytes: Option<u64>,
    pub outer_tx_bytes: Option<u64>,
    pub outer_rx_packets: Option<u64>,
    pub outer_tx_packets: Option<u64>,
    pub dropped_packets: Option<u64>,
    pub active_packet_routes: Option<u64>,
    pub active_stream_flows: Option<u64>,
    pub active_datagram_flows: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointSnapshot {
    pub endpoint_id: String,
    pub tag: String,
    pub protocol: String,
    pub inbound_tags: Vec<String>,
    pub outbound_tags: Vec<String>,
    pub supported: EndpointCapabilities,
    #[serde(default)]
    pub configuration: EndpointConfiguration,
    pub enabled: bool,
    pub allowed: EndpointDirections,
    pub effective: EndpointDirections,
    pub state: EndpointRuntimeState,
    pub health: EndpointHealthState,
    pub state_source: EndpointStateSource,
    pub core_instance_id: String,
    pub config_revision: u64,
    pub intent_revision: u64,
    pub generation: Option<u64>,
    pub observed_at_unix_ms: u64,
    pub started_at_unix_ms: Option<u64>,
    pub counters: EndpointCounters,
    #[serde(default)]
    pub stats_epoch: Option<String>,
    #[serde(default)]
    pub stats_epoch_started_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<EndpointRecovery>,
    pub last_error: Option<ErrorDetail>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointListQuery {
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointGetQuery {
    pub endpoint_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointListSnapshot {
    pub endpoints: Vec<EndpointSnapshot>,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EndpointDetailsSnapshot {
    pub endpoint_id: String,
    #[serde(default)]
    pub core_instance_id: String,
    #[serde(default)]
    pub config_revision: u64,
    #[serde(default)]
    pub observed_at_unix_ms: u64,
    pub generation: Option<u64>,
    pub schema_id: String,
    pub schema_version: u32,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointPersistence {
    #[default]
    RuntimeOnly,
    SourceFile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointSetStateCommand {
    pub endpoint_id: String,
    pub enabled: bool,
    #[serde(default)]
    pub persistence: EndpointPersistence,
    #[serde(default)]
    pub expected_intent_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_core_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointSetDirectionsCommand {
    pub endpoint_id: String,
    pub directions: EndpointDirections,
    #[serde(default)]
    pub persistence: EndpointPersistence,
    #[serde(default)]
    pub expected_intent_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_core_instance_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointOperationCommand {
    pub endpoint_id: String,
    #[serde(default)]
    pub expected_intent_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_core_instance_id: Option<String>,
}

/// Reconciliation progress; successful recovery does not imply peer reachability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointRecovery {
    pub network_generation: u64,
    pub phase: EndpointRecoveryPhase,
    pub observed_at_unix_ms: u64,
    pub retry_after_ms: Option<u64>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointRecoveryPhase {
    Preparing,
    Retrying,
    Recovered,
    Superseded,
}
