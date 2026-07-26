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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowFailureObservation {
    pub stage: String,
    pub code: Option<String>,
    pub message: String,
    pub remote: Option<FlowRemoteEndpoint>,
}
