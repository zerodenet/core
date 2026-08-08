use serde::de::Error as _;
use serde::{Deserialize, Serialize};

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
    ActiveFlows(FlowListQuery),
    RecentFlows(FlowListQuery),
    Flow(FlowGetQuery),
    Policies(PoliciesQuery),
    Policy(PolicyGetQuery),
    Diagnostics(DiagnosticsQuery),
    Sinks(SinksQuery),
    TunStatus(TunStatusQuery),
    /// Catch-all for unknown query types from newer clients.
    Unknown(serde_json::Value),
}

impl Serialize for QueryRequest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let json = match self {
            Self::Capabilities(_) => serde_json::json!({ "capabilities": {} }),
            Self::Health(_) => serde_json::json!({ "health": {} }),
            Self::Config(_) => serde_json::json!({ "config": {} }),
            Self::Runtime(_) => serde_json::json!({ "runtime": {} }),
            Self::Stats(_) => serde_json::json!({ "stats": {} }),
            Self::ActiveFlows(v) => serde_json::json!({ "active_flows": v }),
            Self::RecentFlows(v) => serde_json::json!({ "recent_flows": v }),
            Self::Flow(v) => serde_json::json!({ "flow": v }),
            Self::Policies(_) => serde_json::json!({ "policies": {} }),
            Self::Policy(v) => serde_json::json!({ "policy": v }),
            Self::Diagnostics(_) => serde_json::json!({ "diagnostics": {} }),
            Self::Sinks(_) => serde_json::json!({ "sinks": {} }),
            Self::TunStatus(_) => serde_json::json!({ "tun_status": {} }),
            Self::Unknown(v) => v.clone(),
        };
        json.serialize(serializer)
    }
}

impl QueryRequest {
    pub fn required_permission(&self) -> Permission {
        Permission::Read
    }
}

impl<'de> Deserialize<'de> for QueryRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let Some(obj) = value.as_object() else {
            return Err(D::Error::custom("expected JSON object for QueryRequest"));
        };
        let Some((key, inner)) = obj.iter().next() else {
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
            "active_flows" => serde_json::from_value(inner.clone())
                .map(Self::ActiveFlows)
                .map_err(D::Error::custom),
            "recent_flows" => serde_json::from_value(inner.clone())
                .map(Self::RecentFlows)
                .map_err(D::Error::custom),
            "flow" => serde_json::from_value(inner.clone())
                .map(Self::Flow)
                .map_err(D::Error::custom),
            "policies" => Ok(Self::Policies(PoliciesQuery)),
            "policy" => serde_json::from_value(inner.clone())
                .map(Self::Policy)
                .map_err(D::Error::custom),
            "diagnostics" => Ok(Self::Diagnostics(DiagnosticsQuery)),
            "sinks" => Ok(Self::Sinks(SinksQuery)),
            "tun_status" => Ok(Self::TunStatus(TunStatusQuery)),
            _ => Ok(Self::Unknown(value)),
        }
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
    ActiveFlows(Vec<crate::FlowSnapshot>),
    RecentFlows(Vec<crate::CompletedFlowSnapshot>),
    Flow(crate::FlowSnapshot),
    Policies(Vec<crate::PolicySnapshot>),
    Policy(crate::PolicySnapshot),
    Diagnostics(serde_json::Value),
    Sinks(SinkStatusSnapshot),
    TunStatus(TunStatusSnapshot),
    /// Catch-all for unknown response types from newer servers.
    /// Preserves the raw JSON so consumers can inspect it if needed.
    Unknown(serde_json::Value),
}

impl Serialize for QueryResponse {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let json = match self {
            Self::Capabilities(v) => serde_json::json!({ "capabilities": v }),
            Self::Health(v) => serde_json::json!({ "health": v }),
            Self::Config(v) => serde_json::json!({ "config": v }),
            Self::Runtime(v) => serde_json::json!({ "runtime": v }),
            Self::Stats(v) => serde_json::json!({ "stats": v }),
            Self::ActiveFlows(v) => serde_json::json!({ "active_flows": v }),
            Self::RecentFlows(v) => serde_json::json!({ "recent_flows": v }),
            Self::Flow(v) => serde_json::json!({ "flow": v }),
            Self::Policies(v) => serde_json::json!({ "policies": v }),
            Self::Policy(v) => serde_json::json!({ "policy": v }),
            Self::Diagnostics(v) => serde_json::json!({ "diagnostics": v }),
            Self::Sinks(v) => serde_json::json!({ "sinks": v }),
            Self::TunStatus(v) => serde_json::json!({ "tun_status": v }),
            Self::Unknown(v) => v.clone(),
        };
        json.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for QueryResponse {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let Some(obj) = value.as_object() else {
            return Err(D::Error::custom("expected JSON object for QueryResponse"));
        };
        let Some((key, inner)) = obj.iter().next() else {
            return Err(D::Error::custom(
                "expected non-empty object for QueryResponse",
            ));
        };
        match key.as_str() {
            "capabilities" => serde_json::from_value(inner.clone())
                .map(Self::Capabilities)
                .map_err(D::Error::custom),
            "health" => serde_json::from_value(inner.clone())
                .map(Self::Health)
                .map_err(D::Error::custom),
            "config" => serde_json::from_value(inner.clone())
                .map(Self::Config)
                .map_err(D::Error::custom),
            "runtime" => serde_json::from_value(inner.clone())
                .map(Self::Runtime)
                .map_err(D::Error::custom),
            "stats" => serde_json::from_value(inner.clone())
                .map(Self::Stats)
                .map_err(D::Error::custom),
            "active_flows" => serde_json::from_value(inner.clone())
                .map(Self::ActiveFlows)
                .map_err(D::Error::custom),
            "recent_flows" => serde_json::from_value(inner.clone())
                .map(Self::RecentFlows)
                .map_err(D::Error::custom),
            "flow" => serde_json::from_value(inner.clone())
                .map(Self::Flow)
                .map_err(D::Error::custom),
            "policies" => serde_json::from_value(inner.clone())
                .map(Self::Policies)
                .map_err(D::Error::custom),
            "policy" => serde_json::from_value(inner.clone())
                .map(Self::Policy)
                .map_err(D::Error::custom),
            "diagnostics" => serde_json::from_value(inner.clone())
                .map(Self::Diagnostics)
                .map_err(D::Error::custom),
            "sinks" => serde_json::from_value(inner.clone())
                .map(Self::Sinks)
                .map_err(D::Error::custom),
            "tun_status" => serde_json::from_value(inner.clone())
                .map(Self::TunStatus)
                .map_err(D::Error::custom),
            _ => Ok(Self::Unknown(value)),
        }
    }
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkStatusSnapshot {
    #[serde(default)]
    pub sinks: Vec<SinkStatus>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStatusQuery;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TunStatusSnapshot {
    #[serde(default)]
    pub running: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtu: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}
