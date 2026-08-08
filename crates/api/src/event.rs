use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::EVENT_SCHEMA_ID;

pub mod event_type {
    pub const FLOW_STARTED: &str = "flow.started";
    pub const FLOW_ROUTED: &str = "flow.routed";
    pub const FLOW_UPDATED: &str = "flow.updated";
    pub const FLOW_COMPLETED: &str = "flow.completed";
    pub const FLOW_SNAPSHOT: &str = "flow.snapshot";

    pub const POLICY_SELECTED: &str = "policy.selected";
    pub const POLICY_PROBE_COMPLETED: &str = "policy.probe.completed";
    pub const POLICY_PASSIVE_RELAY_HEALTH_CHANGED: &str = "policy.passive_relay_health.changed";

    pub const STATS_SAMPLED: &str = "stats.sampled";

    pub const CONFIG_CHANGED: &str = "config.changed";

    pub const ENGINE_STARTED: &str = "engine.started";
    pub const ENGINE_STOPPED: &str = "engine.stopped";
    pub const ENGINE_WARNING: &str = "engine.warning";
    pub const IPC_CONNECTED: &str = "ipc.connected";
    pub const IPC_DISCONNECTED: &str = "ipc.disconnected";

    pub const ALL: &[&str] = &[
        FLOW_STARTED,
        FLOW_ROUTED,
        FLOW_UPDATED,
        FLOW_COMPLETED,
        FLOW_SNAPSHOT,
        POLICY_SELECTED,
        POLICY_PROBE_COMPLETED,
        POLICY_PASSIVE_RELAY_HEALTH_CHANGED,
        STATS_SAMPLED,
        CONFIG_CHANGED,
        ENGINE_STARTED,
        ENGINE_STOPPED,
        ENGINE_WARNING,
        IPC_CONNECTED,
        IPC_DISCONNECTED,
    ];

    pub fn is_known(value: &str) -> bool {
        ALL.contains(&value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassiveRelayHealthState {
    Quarantined,
    HalfOpen,
    Healthy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassiveRelayHealthChangedPayload {
    pub policy_tag: String,
    pub member_tag: String,
    pub target: String,
    pub port: u16,
    pub state: PassiveRelayHealthState,
    pub quarantine_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiEvent<P = serde_json::Value> {
    pub schema_id: String,
    pub event_id: String,
    pub event_type: String,
    pub occurred_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core_instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_revision: Option<u64>,
    pub source_id: Option<String>,
    pub sequence: Option<u64>,
    pub principal_key: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub payload: P,
}

impl<P> ApiEvent<P> {
    pub fn new(
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        occurred_at_unix_ms: u64,
        payload: P,
    ) -> Self {
        Self {
            schema_id: EVENT_SCHEMA_ID.to_owned(),
            event_id: event_id.into(),
            event_type: event_type.into(),
            occurred_at_unix_ms,
            core_instance_id: None,
            config_revision: None,
            source_id: None,
            sequence: None,
            principal_key: None,
            labels: BTreeMap::new(),
            payload,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventFilter {
    pub event_types: Vec<String>,
    pub principal_keys: Vec<String>,
    pub inbound_tags: Vec<String>,
}

/// Events retained after a consumer-provided sequence cursor.
///
/// `has_gap` tells a reconnecting consumer that the source evicted at least
/// one event after `requested_after`; the consumer must rebuild state from a
/// query or synchronization snapshot before applying further deltas.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EventReplay {
    /// Identity of the engine instance serving this replay. Consumers must
    /// discard a cursor captured from a different instance.
    #[serde(default)]
    pub core_instance_id: String,
    pub requested_after: u64,
    pub actual_from: u64,
    pub has_gap: bool,
    pub events: Vec<ApiEvent<serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishResult {
    pub delivered: bool,
    pub retryable: bool,
    pub message: Option<String>,
}

impl PublishResult {
    pub fn delivered() -> Self {
        Self {
            delivered: true,
            retryable: false,
            message: None,
        }
    }
}
