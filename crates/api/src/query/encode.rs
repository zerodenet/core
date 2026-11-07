//! Serialize the same wire keys directly from their original borrowed DTO.
use super::*;
use serde::ser::SerializeMap;
#[derive(Serialize)]
struct Empty {}
fn entry<S: serde::Serializer, T: Serialize>(
    serializer: S,
    key: &str,
    value: &T,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(1))?;
    map.serialize_entry(key, value)?;
    map.end()
}
impl Serialize for QueryRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Capabilities(_) => entry(serializer, "capabilities", &Empty {}),
            Self::Health(_) => entry(serializer, "health", &Empty {}),
            Self::Config(_) => entry(serializer, "config", &Empty {}),
            Self::Runtime(_) => entry(serializer, "runtime", &Empty {}),
            Self::Stats(_) => entry(serializer, "stats", &Empty {}),
            Self::TrafficStat(v) => entry(serializer, "traffic_stat", v),
            Self::TrafficStats(v) => entry(serializer, "traffic_stats", v),
            Self::PrincipalFlows(_) => entry(serializer, "principal_flows", &Empty {}),
            Self::ActiveFlows(v) => entry(serializer, "active_flows", v),
            Self::RecentFlows(v) => entry(serializer, "recent_flows", v),
            Self::Flow(v) => entry(serializer, "flow", v),
            Self::Policies(_) => entry(serializer, "policies", &Empty {}),
            Self::Policy(v) => entry(serializer, "policy", v),
            Self::Diagnostics(_) => entry(serializer, "diagnostics", &Empty {}),
            Self::Sinks(_) => entry(serializer, "sinks", &Empty {}),
            Self::TunStatus(_) => entry(serializer, "tun_status", &Empty {}),
            Self::PacketRoutes(v) => entry(serializer, "packet_routes", v),
            Self::PacketRoute(v) => entry(serializer, "packet_route", v),
            Self::Endpoints(v) => entry(serializer, "endpoints", v),
            Self::Endpoint(v) => entry(serializer, "endpoint", v),
            Self::EndpointDetails(v) => entry(serializer, "endpoint_details", v),
            Self::Unknown(v) => v.serialize(serializer),
        }
    }
}

impl Serialize for QueryResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Capabilities(v) => entry(serializer, "capabilities", v),
            Self::Health(v) => entry(serializer, "health", v),
            Self::Config(v) => entry(serializer, "config", v),
            Self::Runtime(v) => entry(serializer, "runtime", v),
            Self::Stats(v) => entry(serializer, "stats", v),
            Self::TrafficStat(v) => entry(serializer, "traffic_stat", v),
            Self::TrafficStats(v) => entry(serializer, "traffic_stats", v),
            Self::PrincipalFlows(v) => entry(serializer, "principal_flows", v),
            Self::ActiveFlows(v) => entry(serializer, "active_flows", v),
            Self::RecentFlows(v) => entry(serializer, "recent_flows", v),
            Self::Flow(v) => entry(serializer, "flow", v),
            Self::Policies(v) => entry(serializer, "policies", v),
            Self::Policy(v) => entry(serializer, "policy", v),
            Self::Diagnostics(v) => entry(serializer, "diagnostics", v),
            Self::Sinks(v) => entry(serializer, "sinks", v),
            Self::TunStatus(v) => entry(serializer, "tun_status", v),
            Self::PacketRoutes(v) => entry(serializer, "packet_routes", v),
            Self::PacketRoute(v) => entry(serializer, "packet_route", v),
            Self::Endpoints(v) => entry(serializer, "endpoints", v),
            Self::Endpoint(v) => entry(serializer, "endpoint", v),
            Self::EndpointDetails(v) => entry(serializer, "endpoint_details", v),
            Self::Unknown(v) => v.serialize(serializer),
        }
    }
}
