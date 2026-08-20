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
    pub selected_interface: Option<FlowNetworkInterfaceObservation>,
    pub route_lookup: Option<FlowRouteLookupObservation>,
    pub socket_binding: Option<FlowSocketBindingObservation>,
    pub connect_stage: Option<String>,
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
