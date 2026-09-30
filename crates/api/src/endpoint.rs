//! Controller-neutral network resource observation and management contracts.

use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub operations: Vec<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointOperationCommand {
    pub endpoint_id: String,
    #[serde(default)]
    pub expected_intent_revision: Option<u64>,
}
