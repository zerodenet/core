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
