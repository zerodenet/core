//! Runtime-neutral session route, path, endpoint, and failure observations.

#[derive(serde::Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowRouteObservation {
    pub mode: String,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_rule: Option<MatchedRouteRule>,
    pub selection_chain: Vec<String>,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct MatchedRouteRule {
    pub index: usize,
    pub condition: String,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowRemoteEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowPathObservation {
    pub outbound_protocol: Option<String>,
    pub remote: Option<FlowRemoteEndpoint>,
    pub relay_chain: Vec<(String, String)>,
    pub network: Option<FlowNetworkObservation>,
}

#[derive(serde::Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowNetworkObservation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_address: Option<FlowRemoteEndpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_address: Option<FlowRemoteEndpoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resolved_candidates: Vec<FlowRemoteEndpoint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub connection_attempts: Vec<FlowConnectionAttemptObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_family_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_family_fallback: Option<FlowAddressFamilyFallbackObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_interface: Option<FlowNetworkInterfaceObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<FlowEgressObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_lookup: Option<FlowRouteLookupObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub socket_binding: Option<FlowSocketBindingObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_stage: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowConnectionAttemptObservation {
    pub remote_address: FlowRemoteEndpoint,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_address: Option<FlowRemoteEndpoint>,
    pub stage: String,
    pub outcome: String,
    pub interface_bound: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os_error: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowAddressFamilyFallbackObservation {
    pub from: String,
    pub to: String,
    pub reason: String,
    pub trigger_egress_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowEgressObservation {
    pub generation: u64,
    pub address_family: String,
    pub tun_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configured_interface: Option<FlowNetworkInterfaceObservation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowNetworkInterfaceObservation {
    pub name: String,
    pub index: u32,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowRouteLookupObservation {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowSocketBindingObservation {
    pub mode: String,
    pub reason: String,
    pub interface_bound: bool,
}

#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowFailureObservation {
    pub stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote: Option<FlowRemoteEndpoint>,
}
