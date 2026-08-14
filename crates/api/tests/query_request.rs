//! Integration tests for `QueryRequest` deserialization and round-trip.
//!
//! These verify the custom `Deserialize` / `Serialize` implementations
//! match the documented wire format.  See `crates/api/src/query.rs` for
//! the implementation.

use zero_api::{
    CapabilitiesQuery, ConfigQuery, DiagnosticsQuery, FlowFilter, FlowListQuery, HealthQuery,
    PoliciesQuery, PrincipalFlowsQuery, QueryRequest, RuntimeQuery, SinksQuery, StatsQuery,
    TunStatusQuery,
};

// ── Unit-struct query types: deserialize from {"key":{}} ──────────
//
// These are the 9 query types that carry no parameters.  The wire
// format sends `{}` as the inner value, but serde_json's derived
// Deserialize rejects empty objects for unit structs.  The custom
// Deserialize impl for QueryRequest handles this by constructing
// the unit struct directly instead of going through from_value.

#[test]
fn deser_capabilities() {
    let qr: QueryRequest = serde_json::from_str(r#"{"capabilities":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Capabilities(CapabilitiesQuery));
}

#[test]
fn deser_health() {
    let qr: QueryRequest = serde_json::from_str(r#"{"health":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Health(HealthQuery));
}

#[test]
fn deser_config() {
    let qr: QueryRequest = serde_json::from_str(r#"{"config":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Config(ConfigQuery));
}

#[test]
fn deser_runtime() {
    let qr: QueryRequest = serde_json::from_str(r#"{"runtime":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Runtime(RuntimeQuery));
}

#[test]
fn deser_stats() {
    let qr: QueryRequest = serde_json::from_str(r#"{"stats":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Stats(StatsQuery));
}

#[test]
fn deser_principal_flows() {
    let qr: QueryRequest = serde_json::from_str(r#"{"principal_flows":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::PrincipalFlows(PrincipalFlowsQuery));
}

#[test]
fn deser_policies() {
    let qr: QueryRequest = serde_json::from_str(r#"{"policies":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Policies(PoliciesQuery));
}

#[test]
fn deser_diagnostics() {
    let qr: QueryRequest = serde_json::from_str(r#"{"diagnostics":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Diagnostics(DiagnosticsQuery));
}

#[test]
fn deser_sinks() {
    let qr: QueryRequest = serde_json::from_str(r#"{"sinks":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::Sinks(SinksQuery));
}

#[test]
fn deser_tun_status() {
    let qr: QueryRequest = serde_json::from_str(r#"{"tun_status":{}}"#).unwrap();
    assert_eq!(qr, QueryRequest::TunStatus(TunStatusQuery));
}

// ── Non-unit query types (have real fields) ────────────────────────

#[test]
fn deser_active_flows_with_limit() {
    let qr: QueryRequest =
        serde_json::from_str(r#"{"active_flows":{"limit":5,"filter":{}}}"#).unwrap();
    match qr {
        QueryRequest::ActiveFlows(f) => {
            assert_eq!(f.limit, Some(5));
        }
        _ => panic!("expected ActiveFlows, got {:?}", qr),
    }
}

#[test]
fn deser_active_flows_default_filter() {
    // FlowFilter has all Option fields — `"filter":{}` should deserialize
    let qr: QueryRequest = serde_json::from_str(r#"{"active_flows":{"filter":{}}}"#).unwrap();
    assert!(matches!(qr, QueryRequest::ActiveFlows(_)));
}

#[test]
fn deser_recent_flows() {
    let qr: QueryRequest =
        serde_json::from_str(r#"{"recent_flows":{"limit":10,"filter":{}}}"#).unwrap();
    assert!(matches!(qr, QueryRequest::RecentFlows(_)));
}

#[test]
fn deser_flow_by_id() {
    let qr: QueryRequest = serde_json::from_str(r#"{"flow":{"flow_id":"abc-123"}}"#).unwrap();
    match qr {
        QueryRequest::Flow(f) => assert_eq!(f.flow_id, "abc-123"),
        _ => panic!("expected Flow, got {:?}", qr),
    }
}

#[test]
fn deser_policy() {
    let qr: QueryRequest = serde_json::from_str(r#"{"policy":{"policy_tag":"proxy"}}"#).unwrap();
    match qr {
        QueryRequest::Policy(p) => assert_eq!(p.policy_tag, "proxy"),
        _ => panic!("expected Policy, got {:?}", qr),
    }
}

// ── Unknown query type (forward-compat) ────────────────────────────

#[test]
fn deser_unknown_query_type() {
    let qr: QueryRequest = serde_json::from_str(r#"{"future_query":{"some":"data"}}"#).unwrap();
    assert!(matches!(qr, QueryRequest::Unknown(_)));
}

// ── Error cases ────────────────────────────────────────────────────

#[test]
fn deser_non_object_is_error() {
    let err = serde_json::from_str::<QueryRequest>(r#"[]"#).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("object"), "unexpected error: {}", msg);
}

#[test]
fn deser_empty_object_is_error() {
    let err = serde_json::from_str::<QueryRequest>(r#"{}"#).unwrap_err();
    assert!(err.to_string().contains("expected non-empty object"));
}

// ── Round-trip ─────────────────────────────────────────────────────

#[test]
fn round_trip_unit_structs() {
    let cases: Vec<QueryRequest> = vec![
        QueryRequest::Capabilities(CapabilitiesQuery),
        QueryRequest::Health(HealthQuery),
        QueryRequest::Config(ConfigQuery),
        QueryRequest::Runtime(RuntimeQuery),
        QueryRequest::Stats(StatsQuery),
        QueryRequest::PrincipalFlows(PrincipalFlowsQuery),
        QueryRequest::Policies(PoliciesQuery),
        QueryRequest::Diagnostics(DiagnosticsQuery),
        QueryRequest::Sinks(SinksQuery),
        QueryRequest::TunStatus(TunStatusQuery),
    ];
    for original in cases {
        let json = serde_json::to_string(&original).unwrap();
        let rt: QueryRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, rt, "round-trip failed for: {}", json);
    }
}

#[test]
fn round_trip_with_fields() {
    let original = QueryRequest::ActiveFlows(FlowListQuery {
        limit: Some(3),
        filter: FlowFilter::default(),
    });
    let json = serde_json::to_string(&original).unwrap();
    let rt: QueryRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(original, rt);
}
