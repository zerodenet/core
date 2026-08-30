//! Runtime-neutral session route, path, endpoint, and failure observations.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowRouteObservation {
    pub mode: String,
    pub action: String,
    pub target: Option<String>,
    pub matched_rule: Option<MatchedRouteRule>,
    pub selection_chain: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRouteRule {
    pub index: usize,
    pub condition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowNetworkObservation {
    pub local_address: Option<FlowRemoteEndpoint>,
    pub remote_address: Option<FlowRemoteEndpoint>,
    pub resolved_candidates: Vec<FlowRemoteEndpoint>,
    pub connection_attempts: Vec<FlowConnectionAttemptObservation>,
    pub address_family_policy: Option<String>,
    pub address_family_fallback: Option<FlowAddressFamilyFallbackObservation>,
    pub selected_interface: Option<FlowNetworkInterfaceObservation>,
    pub egress: Option<FlowEgressObservation>,
    pub route_lookup: Option<FlowRouteLookupObservation>,
    pub socket_binding: Option<FlowSocketBindingObservation>,
    pub connect_stage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConnectionAttemptObservation {
    pub remote_address: FlowRemoteEndpoint,
    pub local_address: Option<FlowRemoteEndpoint>,
    pub stage: String,
    pub outcome: String,
    pub interface_bound: bool,
    pub error_kind: Option<String>,
    pub os_error: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowAddressFamilyFallbackObservation {
    pub from: String,
    pub to: String,
    pub reason: String,
    pub trigger_egress_generation: u64,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlowEgressObservation {
    pub generation: u64,
    pub address_family: String,
    pub tun_active: bool,
    pub configured_interface: Option<FlowNetworkInterfaceObservation>,
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNetworkInterfaceObservation {
    pub name: String,
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowRouteLookupObservation {
    pub status: String,
    pub source_address: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowSocketBindingObservation {
    pub mode: String,
    pub reason: String,
    pub interface_bound: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowFailureObservation {
    pub stage: String,
    pub code: Option<String>,
    pub message: String,
    pub remote: Option<FlowRemoteEndpoint>,
}
