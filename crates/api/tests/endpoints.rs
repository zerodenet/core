use serde_json::json;
use zero_api::{
    CommandRequest, EndpointCounters, EndpointDirections, EndpointSnapshot, QueryRequest,
    QueryResponse,
};

#[test]
fn endpoint_queries_and_responses_roundtrip_with_unknown_counters() {
    for input in [
        json!({"endpoints": {"offset": 2, "limit": 10}}),
        json!({"endpoint": {"endpoint_id": "endpoint:a"}}),
        json!({"endpoint_details": {"endpoint_id": "legacy:inbound:a"}}),
    ] {
        let request: QueryRequest = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), input);
    }
    let endpoint = EndpointSnapshot {
        endpoint_id: "endpoint:a".into(),
        ..Default::default()
    };
    let response = QueryResponse::Endpoint(endpoint.clone());
    let encoded = serde_json::to_value(&response).unwrap();
    assert!(encoded["endpoint"]["counters"]["inner_rx_bytes"].is_null());
    assert!(encoded["endpoint"]["generation"].is_null());
    assert_eq!(
        serde_json::from_value::<QueryResponse>(encoded).unwrap(),
        response
    );
    assert_eq!(endpoint.counters, EndpointCounters::default());
}

#[test]
fn endpoint_commands_require_admin_and_default_to_runtime_only() {
    let command: CommandRequest = serde_json::from_value(json!({"method":"endpoints.set_state", "params":{"endpoint_id":"endpoint:a", "enabled":false}})).unwrap();
    assert_eq!(command.required_permission(), zero_api::Permission::Admin);
    let CommandRequest::EndpointSetState(command) = command else {
        panic!("wrong command");
    };
    assert_eq!(
        command.persistence,
        zero_api::EndpointPersistence::RuntimeOnly
    );
    assert!(command.expected_core_instance_id.is_none());
    assert!(
        !EndpointDirections::outbound_only().permits(EndpointDirections {
            inbound: true,
            outbound: false
        })
    );
    assert!(serde_json::from_value::<QueryRequest>(
        json!({"endpoint": {"endpoint_id": "a", "extra":true}})
    )
    .is_err());
}

#[test]
fn every_endpoint_mutation_accepts_both_optional_preconditions() {
    for (method, extra) in [
        ("endpoints.set_state", json!({"enabled":false})),
        (
            "endpoints.set_directions",
            json!({"directions":{"inbound":false,"outbound":true}}),
        ),
        ("endpoints.restart", json!({})),
        ("endpoints.clear_overrides", json!({})),
    ] {
        let mut params = extra;
        params["endpoint_id"] = json!("opaque-id");
        params["expected_core_instance_id"] = json!("instance-a");
        params["expected_intent_revision"] = json!(9);
        let command: CommandRequest =
            serde_json::from_value(json!({"method":method,"params":params})).unwrap();
        let encoded = serde_json::to_value(command).unwrap();
        assert_eq!(encoded["params"]["expected_core_instance_id"], "instance-a");
        assert_eq!(encoded["params"]["expected_intent_revision"], 9);
    }
}

#[test]
fn older_endpoint_snapshots_default_missing_metadata_without_inventing_a_source() {
    let mut encoded = serde_json::to_value(EndpointSnapshot::default()).unwrap();
    encoded.as_object_mut().unwrap().remove("configuration");
    encoded["supported"]
        .as_object_mut()
        .unwrap()
        .remove("operation_capabilities");
    let endpoint: EndpointSnapshot = serde_json::from_value(encoded).unwrap();
    assert_eq!(
        endpoint.configuration.origin,
        zero_api::EndpointConfigurationOrigin::Unknown
    );
    assert!(!endpoint.configuration.source_file.available);
    assert_eq!(endpoint.configuration.source_file.writable, None);
    assert!(endpoint.supported.operation_capabilities.is_empty());
}
