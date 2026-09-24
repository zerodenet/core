/// Forward-compatibility tests: verify that adding new fields to snapshot
/// types does NOT break deserialization for old consumers.
///
/// These tests simulate what happens when a newer server sends responses
/// with extra fields to an older client that deserializes with the same
/// type definition — `#[serde(default)]` ensures silent default-filling.
use serde_json::json;
use zero_api::{
    ConfigSnapshot, FlowSnapshot, HealthSnapshot, QueryResponse, RuntimeSnapshot, StatsSnapshot,
};

/// New server adds a field; old client's snapshot struct should still parse.
#[test]
fn config_snapshot_tolerates_unknown_fields() {
    let json = json!({
        "mode": {"kind": "rule"},
        "rule_count": 5,
        "listeners": [],
        "outbounds": [],
        "outbound_groups": [],
        "future_field_added_in_v2": "should be ignored"
    });
    let result: ConfigSnapshot = serde_json::from_value(json).expect("should parse");
    assert_eq!(result.rule_count, 5);
}

#[test]
fn runtime_snapshot_tolerates_unknown_fields() {
    let json = json!({
        "stats": {
            "active_sessions": 0,
            "future_stats_counter_added_later": 42
        },
        "log_level": "debug",
        "log_files": [],
        "active_sessions": [],
        "recent_completed_sessions": [],
        "future_top_level_field": null
    });
    let result: RuntimeSnapshot = serde_json::from_value(json).expect("should parse");
    assert_eq!(result.log_level, "debug");
}

#[test]
fn flow_snapshot_tolerates_missing_optional_fields() {
    // Minimal JSON — only present fields, simulating an old server before
    // process_id / process_name were added.
    let json = json!({
        "id": 42,
        "target": {"family": "ipv4", "value": "1.2.3.4"},
        "port": 443,
        "protocol": "socks5",
        "network": "tcp",
        "mode": "rule",
        "started_at_unix_ms": 1000,
        "last_activity_at_unix_ms": 2000,
        "bytes_up": 100,
        "bytes_down": 200,
        "inbound_rx_bytes": 100,
        "inbound_tx_bytes": 200,
        "outbound_rx_bytes": 200,
        "outbound_tx_bytes": 100,
        "throughput_up_bps": 0,
        "throughput_down_bps": 0
    });
    let result: FlowSnapshot = serde_json::from_value(json).expect("should parse");
    assert_eq!(result.id, 42);
    assert_eq!(result.process_id, None); // Not in JSON → default
    assert_eq!(result.process_name, None);
    assert_eq!(result.inbound_tag, None); // Optional fields default to None
}

#[test]
fn health_snapshot_defaults_missing_fields() {
    let json = json!({
        "engine_build_id": "0.0.10",
        "healthy": true
    });
    let result: HealthSnapshot = serde_json::from_value(json).expect("should parse");
    assert_eq!(result.engine_build_id, "0.0.10");
    assert!(result.healthy);
    assert_eq!(result.started_at_unix_ms, None); // Missing → default
    assert!(result.outbound_devices.is_empty());
}

#[test]
fn health_snapshot_accepts_endpoint_resolution_state() {
    let result: HealthSnapshot = serde_json::from_value(json!({
        "outbound_devices": [{
            "tag": "wg",
            "peer_index": 0,
            "state": "endpoint_unresolved",
            "endpoint_resolution_failed": true
        }]
    }))
    .expect("parse endpoint resolution state");
    assert_eq!(
        result.outbound_devices[0].state,
        zero_api::OutboundDeviceHealthState::EndpointUnresolved
    );
    assert!(result.outbound_devices[0].endpoint_resolution_failed);
}

#[test]
fn stats_snapshot_tolerates_new_per_outbound_flags() {
    let json = json!({
        "active_sessions": 5,
        "completed_sessions": 10,
        "per_outbound": [
            ["direct", {"flows": 3, "bytes_up": 100, "bytes_down": 200}],
            ["proxy", {"flows": 2, "bytes_up": 50, "bytes_down": 75, "future_flag_added_v3": true}]
        ],
        "udp_upstream": {
            "active_associations": 1,
            "packets_sent": 10,
            "packets_received": 8,
            "future_udp_metric": 99
        }
    });
    let result: StatsSnapshot = serde_json::from_value(json).expect("should parse");
    assert_eq!(result.active_sessions, 5);
    assert_eq!(result.per_outbound.len(), 2);
    assert_eq!(result.udp_upstream.packets_sent, 10);
}

/// QueryResponse enum maintains backwards compatibility: valid variant or
/// deserialization error is surfaced (NOT silently ignored).
#[test]
fn query_response_parses_known_variants() {
    let value = serde_json::to_value(QueryResponse::Runtime(RuntimeSnapshot::default()))
        .expect("serialize");
    let back: QueryResponse = serde_json::from_value(value).expect("deserialize");
    match back {
        QueryResponse::Runtime(_) => {} // OK
        _ => panic!("expected Runtime variant"),
    }
}
