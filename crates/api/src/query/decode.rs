//! Move recognized owned JSON values; preserve the entire unknown object.
use super::*;
use serde::de::Error as _;
impl<'de> Deserialize<'de> for QueryRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let Some(obj) = value.as_object_mut() else {
            return Err(D::Error::custom("expected JSON object for QueryRequest"));
        };
        let Some((key, inner)) = obj.iter_mut().next() else {
            return Err(D::Error::custom(
                "expected non-empty object for QueryRequest",
            ));
        };
        // Unit-struct query types carry no parameters — the inner value is
        // always `{}` on the wire (e.g. `{"capabilities":{}}`), which
        // serde_json's derived Deserialize rejects for unit structs (it
        // only accepts `null`).  Construct them directly instead of going
        // through serde_json::from_value.
        //
        // Non-unit query types (active_flows, recent_flows, flow, policy)
        // have real fields and are deserialized normally from the inner
        // JSON value.
        match key.as_str() {
            "capabilities" => Ok(Self::Capabilities(CapabilitiesQuery)),
            "health" => Ok(Self::Health(HealthQuery)),
            "config" => Ok(Self::Config(ConfigQuery)),
            "runtime" => Ok(Self::Runtime(RuntimeQuery)),
            "stats" => Ok(Self::Stats(StatsQuery)),
            "traffic_stat" => serde_json::from_value(std::mem::take(inner))
                .map(Self::TrafficStat)
                .map_err(D::Error::custom),
            "traffic_stats" => serde_json::from_value(std::mem::take(inner))
                .map(Self::TrafficStats)
                .map_err(D::Error::custom),
            "principal_flows" => Ok(Self::PrincipalFlows(PrincipalFlowsQuery)),
            "active_flows" => serde_json::from_value(std::mem::take(inner))
                .map(Self::ActiveFlows)
                .map_err(D::Error::custom),
            "recent_flows" => serde_json::from_value(std::mem::take(inner))
                .map(Self::RecentFlows)
                .map_err(D::Error::custom),
            "flow" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Flow)
                .map_err(D::Error::custom),
            "policies" => Ok(Self::Policies(PoliciesQuery)),
            "policy" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Policy)
                .map_err(D::Error::custom),
            "diagnostics" => Ok(Self::Diagnostics(DiagnosticsQuery)),
            "sinks" => Ok(Self::Sinks(SinksQuery)),
            "tun_status" => Ok(Self::TunStatus(TunStatusQuery)),
            "packet_routes" => serde_json::from_value(std::mem::take(inner))
                .map(Self::PacketRoutes)
                .map_err(D::Error::custom),
            "packet_route" => serde_json::from_value(std::mem::take(inner))
                .map(Self::PacketRoute)
                .map_err(D::Error::custom),
            "endpoints" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Endpoints)
                .map_err(D::Error::custom),
            "endpoint" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Endpoint)
                .map_err(D::Error::custom),
            "endpoint_details" => serde_json::from_value(std::mem::take(inner))
                .map(Self::EndpointDetails)
                .map_err(D::Error::custom),
            _ => Ok(Self::Unknown(value)),
        }
    }
}

impl<'de> Deserialize<'de> for QueryResponse {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let Some(obj) = value.as_object_mut() else {
            return Err(D::Error::custom("expected JSON object for QueryResponse"));
        };
        let Some((key, inner)) = obj.iter_mut().next() else {
            return Err(D::Error::custom(
                "expected non-empty object for QueryResponse",
            ));
        };
        match key.as_str() {
            "capabilities" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Capabilities)
                .map_err(D::Error::custom),
            "health" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Health)
                .map_err(D::Error::custom),
            "config" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Config)
                .map_err(D::Error::custom),
            "runtime" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Runtime)
                .map_err(D::Error::custom),
            "traffic_stat" => serde_json::from_value(std::mem::take(inner))
                .map(Self::TrafficStat)
                .map_err(D::Error::custom),
            "traffic_stats" => serde_json::from_value(std::mem::take(inner))
                .map(Self::TrafficStats)
                .map_err(D::Error::custom),
            "stats" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Stats)
                .map_err(D::Error::custom),
            "principal_flows" => serde_json::from_value(std::mem::take(inner))
                .map(Self::PrincipalFlows)
                .map_err(D::Error::custom),
            "active_flows" => serde_json::from_value(std::mem::take(inner))
                .map(Self::ActiveFlows)
                .map_err(D::Error::custom),
            "recent_flows" => serde_json::from_value(std::mem::take(inner))
                .map(Self::RecentFlows)
                .map_err(D::Error::custom),
            "flow" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Flow)
                .map_err(D::Error::custom),
            "policies" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Policies)
                .map_err(D::Error::custom),
            "policy" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Policy)
                .map_err(D::Error::custom),
            "diagnostics" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Diagnostics)
                .map_err(D::Error::custom),
            "sinks" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Sinks)
                .map_err(D::Error::custom),
            "tun_status" => serde_json::from_value(std::mem::take(inner))
                .map(Self::TunStatus)
                .map_err(D::Error::custom),
            "packet_routes" => serde_json::from_value(std::mem::take(inner))
                .map(Self::PacketRoutes)
                .map_err(D::Error::custom),
            "packet_route" => serde_json::from_value(std::mem::take(inner))
                .map(Self::PacketRoute)
                .map_err(D::Error::custom),
            "endpoints" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Endpoints)
                .map_err(D::Error::custom),
            "endpoint" => serde_json::from_value(std::mem::take(inner))
                .map(Self::Endpoint)
                .map_err(D::Error::custom),
            "endpoint_details" => serde_json::from_value(std::mem::take(inner))
                .map(Self::EndpointDetails)
                .map_err(D::Error::custom),
            _ => Ok(Self::Unknown(value)),
        }
    }
}
