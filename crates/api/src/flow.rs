use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventPayload {
    pub flow_id: String,
    pub network: Network,
    pub inbound: EndpointRef,
    pub auth: Option<AuthInfo>,
    pub target: TargetAddress,
    pub route: RouteDecision,
    pub policy: Option<PolicyDecision>,
    pub outbound: Option<EndpointRef>,
    pub traffic: TrafficStats,
    pub timing: FlowTiming,
    pub outcome: FlowOutcome,
    /// Authoritative active-flow count for this event's principal after the
    /// lifecycle transition. Present only when `principal_key` is known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_active_flows: Option<u64>,
    /// Monotonic ordering of principal flow state transitions inside the
    /// session registry. Consumers must use this for state ordering; event
    /// `sequence` remains the delivery/replay order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_registry_revision: Option<u64>,
    /// Time at which the principal count changed in the session registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at_unix_ms: Option<u64>,
    /// Why the flow ended (standard close reason). `None` = normal / unspecified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    /// Canonical connection record shared by GUI, SSE, JSONL, and webhook
    /// consumers. Legacy top-level fields stay on this payload for wire
    /// compatibility while new consumers migrate to this record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<FlowRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowState {
    Opening,
    Active,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowRecord {
    pub flow_id: String,
    pub revision: u64,
    pub state: FlowState,
    pub network: Network,
    pub inbound: EndpointRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<FlowSource>,
    pub target: FlowTarget,
    pub route: FlowRoute,
    pub path: FlowPath,
    pub traffic: TrafficStats,
    pub throughput: FlowThroughput,
    pub timing: FlowRecordTiming,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<FlowResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowSource {
    pub ip: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTarget {
    pub host: String,
    pub port: u16,
    /// Original IP intercepted before domain restoration/sniffing. For a
    /// Fake-IP flow this is the synthetic address returned to the client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fake_ip_reverse_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sniffed_host: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowRoute {
    pub mode: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_rule: Option<MatchedRuleInfo>,
    #[serde(default)]
    pub selection_chain: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedRuleInfo {
    pub index: usize,
    pub condition: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowPath {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound: Option<EndpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<TargetAddress>,
    #[serde(default)]
    pub relay_chain: Vec<EndpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<FlowNetworkContext>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowNetworkContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_address: Option<TargetAddress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_interface: Option<FlowNetworkInterface>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_lookup: Option<FlowRouteLookup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket_binding: Option<FlowSocketBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowNetworkInterface {
    pub name: String,
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowRouteLookup {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowSocketBinding {
    pub mode: String,
    pub reason: String,
    pub interface_bound: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowThroughput {
    pub upload_bps: u64,
    pub download_bps: u64,
    pub sampled_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowRecordTiming {
    pub started_at_unix_ms: u64,
    pub last_activity_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowResult {
    pub outcome: FlowOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FlowFailureInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowFailureInfo {
    pub stage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<TargetAddress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointRef {
    pub tag: String,
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthInfo {
    pub scheme: String,
    pub principal_key: Option<String>,
    pub attributes: BTreeMap<String, String>,
}

impl AuthInfo {
    pub fn new(scheme: impl Into<String>) -> Self {
        Self {
            scheme: scheme.into(),
            principal_key: None,
            attributes: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetAddress {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDecision {
    pub mode: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub tag: String,
    pub kind: String,
    pub selected: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficStats {
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub inbound_rx_bytes: Option<u64>,
    pub inbound_tx_bytes: Option<u64>,
    pub outbound_rx_bytes: Option<u64>,
    pub outbound_tx_bytes: Option<u64>,
    pub packets_up: Option<u64>,
    pub packets_down: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTiming {
    pub started_at_unix_ms: u64,
    pub ended_at_unix_ms: Option<u64>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowOutcome {
    DirectRelayed,
    ChainedRelayed,
    Blocked,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySelectedPayload {
    pub policy_tag: String,
    pub policy_kind: String,
    pub selected: String,
    pub previous: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyProbeCompletedPayload {
    #[serde(default)]
    pub operation_id: String,
    #[serde(default)]
    pub core_instance_id: String,
    #[serde(default)]
    pub config_revision: u64,
    pub policy_tag: String,
    #[serde(default)]
    pub trigger: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub started_at_unix_ms: u64,
    #[serde(default)]
    pub completed_at_unix_ms: u64,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub terminal_status: String,
    pub selected: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<crate::UrlTestSelectionSnapshot>,
    pub members: Vec<PolicyProbeMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyProbeMember {
    pub target_tag: String,
    pub healthy: bool,
    pub latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarningPayload {
    pub code: String,
    pub message: String,
}
