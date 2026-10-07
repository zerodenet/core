//! A PacketRoute is an executed conversation lease, not a persistent policy.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketRouteListQuery {
    #[serde(default)]
    pub offset: usize,
    pub limit: Option<usize>,
    pub endpoint_id: Option<String>,
    pub inbound_tag: Option<String>,
    pub outbound_tag: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketRouteGetQuery {
    pub route_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketRouteCloseCommand {
    pub route_id: String,
    pub expected_core_instance_id: String,
    pub expected_config_revision: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PacketRouteState {
    Active,
    Closing,
    Closed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PacketRouteSnapshot {
    pub route_id: String,
    pub core_instance_id: String,
    pub config_revision: u64,
    pub inbound_tag: String,
    pub outbound_tag: String,
    pub endpoints: Vec<PacketRouteEndpointRef>,
    pub source: String,
    pub destination: String,
    pub ip_protocol: u8,
    pub translated: bool,
    pub started_at_unix_ms: u64,
    pub state: PacketRouteState,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PacketRouteListSnapshot {
    pub core_instance_id: String,
    pub config_revision: u64,
    pub observed_at_unix_ms: u64,
    pub routes: Vec<PacketRouteSnapshot>,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PacketRouteEndpointRef {
    pub endpoint_id: String,
    pub generation: Option<u64>,
}
