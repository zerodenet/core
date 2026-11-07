use serde::{Deserialize, Serialize};
mod decode;
mod encode;

use crate::{ApiCapabilities, Network, Permission, SinkStatus};

// ── QueryRequest ────────────────────────────────────────────────────

/// Typed query request enum with forward-compatible deserialization.
///
/// Unknown query types from newer clients deserialize into `Unknown`
/// instead of causing a serde error. This allows old servers to gracefully
/// reject new query types while preserving the raw request data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryRequest {
    Capabilities(CapabilitiesQuery),
    Health(HealthQuery),
    Config(ConfigQuery),
    Runtime(RuntimeQuery),
    Stats(StatsQuery),
    TrafficStat(crate::TrafficGetQuery),
    TrafficStats(crate::TrafficListQuery),
    PrincipalFlows(PrincipalFlowsQuery),
    ActiveFlows(FlowListQuery),
    RecentFlows(FlowListQuery),
    Flow(FlowGetQuery),
    Policies(PoliciesQuery),
    Policy(PolicyGetQuery),
    Diagnostics(DiagnosticsQuery),
    Sinks(SinksQuery),
    TunStatus(TunStatusQuery),
    PacketRoutes(crate::PacketRouteListQuery),
    PacketRoute(crate::PacketRouteGetQuery),
    Endpoints(crate::EndpointListQuery),
    Endpoint(crate::EndpointGetQuery),
    EndpointDetails(crate::EndpointGetQuery),
    /// Catch-all for unknown query types from newer clients.
    Unknown(serde_json::Value),
}

impl QueryRequest {
    pub fn required_permission(&self) -> Permission {
        Permission::Read
    }
}

// ── QueryResponse ───────────────────────────────────────────────────

/// Typed query response enum with forward-compatible deserialization.
///
/// Unknown response types from newer servers deserialize into `Unknown`
/// instead of causing a serde error.  This allows old consumers to stay
/// alive when connected to newer server versions.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResponse {
    Capabilities(ApiCapabilities),
    Health(HealthSnapshot),
    Config(crate::ConfigSnapshot),
    Runtime(crate::RuntimeSnapshot),
    Stats(crate::StatsSnapshot),
    TrafficStat(crate::TrafficSnapshot),
    TrafficStats(crate::TrafficListSnapshot),
    PrincipalFlows(crate::PrincipalFlowsSnapshot),
    ActiveFlows(Vec<crate::FlowSnapshot>),
    RecentFlows(Vec<crate::CompletedFlowSnapshot>),
    Flow(crate::FlowSnapshot),
    Policies(Vec<crate::PolicySnapshot>),
    Policy(crate::PolicySnapshot),
    Diagnostics(serde_json::Value),
    Sinks(SinkStatusSnapshot),
    TunStatus(TunStatusSnapshot),
    PacketRoutes(crate::PacketRouteListSnapshot),
    PacketRoute(crate::PacketRouteSnapshot),
    Endpoints(crate::EndpointListSnapshot),
    Endpoint(crate::EndpointSnapshot),
    EndpointDetails(crate::EndpointDetailsSnapshot),
    /// Catch-all for unknown response types from newer servers.
    /// Preserves the raw JSON so consumers can inspect it if needed.
    Unknown(serde_json::Value),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilitiesQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalFlowsQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoliciesQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticsQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinksQuery;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowListQuery {
    pub limit: Option<usize>,
    pub filter: FlowFilter,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowFilter {
    pub principal_key: Option<String>,
    pub inbound_tag: Option<String>,
    pub network: Option<Network>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowGetQuery {
    #[serde(default)]
    pub flow_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyGetQuery {
    #[serde(default)]
    pub policy_tag: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthSnapshot {
    #[serde(default)]
    pub engine_build_id: String,
    #[serde(default)]
    pub core_instance_id: String,
    #[serde(default)]
    pub config_revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<u64>,
    #[serde(default)]
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outbound_devices: Vec<OutboundDeviceHealthSnapshot>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundDeviceHealthSnapshot {
    #[serde(default)]
    pub tag: String,
    #[serde(default)]
    pub peer_index: usize,
    #[serde(default)]
    pub state: OutboundDeviceHealthState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_handshake_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_authenticated_packet_age_ms: Option<u64>,
    #[serde(default)]
    pub endpoint_resolution_failed: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundDeviceHealthState {
    #[default]
    NotStarted,
    EndpointUnresolved,
    AwaitingHandshake,
    RecentlyHandshaken,
    Reachable,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkStatusSnapshot {
    #[serde(default)]
    pub sinks: Vec<SinkStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStatusQuery;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TunFamilyEgressAvailability {
    #[default]
    Unknown,
    Available,
    Unavailable,
}

impl TunFamilyEgressAvailability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Available => "available",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunFamilyEgressSnapshot {
    #[serde(default)]
    pub availability: TunFamilyEgressAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStatusSnapshot {
    #[serde(default)]
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    /// All effective interface addresses, including an automatically selected
    /// secondary address in dual-stack mode.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtu: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Local TUN runtime and managed route readiness. This does not assert
    /// end-to-end DNS, Internet, or proxy-node connectivity.
    #[serde(default)]
    pub healthy: bool,
    #[serde(default)]
    pub auto_route: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_cidrs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_cidrs: Vec<String>,
    #[serde(default)]
    pub dual_stack: bool,
    #[serde(default)]
    pub strict_route: bool,
    #[serde(default)]
    pub dns_hijack: bool,
    /// Whether the committed DNS runtime has a Fake-IP allocator available.
    #[serde(default)]
    pub fake_ip_enabled: bool,
    /// Number of UDP/TCP DNS requests intercepted by this TUN lifetime.
    #[serde(default)]
    pub dns_hijacked_queries: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_interface: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_interface_v4: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_interface_v6: Option<String>,
    #[serde(default)]
    pub ipv4_egress: TunFamilyEgressSnapshot,
    #[serde(default)]
    pub ipv6_egress: TunFamilyEgressSnapshot,
    #[serde(default)]
    pub network_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address_family_policy: Option<String>,
    #[serde(default)]
    pub ipv6_to_ipv4_fallbacks: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default)]
    pub managed_by_config: bool,
}
