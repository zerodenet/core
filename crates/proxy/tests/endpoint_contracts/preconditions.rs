#![cfg(feature = "wireguard")]

use crate::{management_support as management, support};

use management::{config, endpoint, handle, ready};
use serde_json::json;
use support::{free_udp_port, spawn_engine};
use zero_api::{
    ApiErrorCode, CommandRequest, CommandService, EndpointPersistence, EndpointRuntimeState,
};
use zero_engine::Engine;
use zero_proxy::Proxy;

#[tokio::test]
async fn restarted_core_rejects_reused_revision_before_noop_persistence_or_resource_work() {
    let initial = config(free_udp_port(), free_udp_port());
    let previous = Engine::new(initial.clone()).unwrap();
    let stale = previous
        .endpoints_snapshot(&Default::default())
        .endpoints
        .remove(0);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&initial).unwrap()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let proxy = Proxy::from_engine(Engine::new_with_config_path(initial, &path).unwrap()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    let current = endpoint(&handle, "a");
    assert_eq!(current.intent_revision, stale.intent_revision);
    assert_ne!(current.core_instance_id, stale.core_instance_id);
    for (method, extra) in [
        (
            "endpoints.set_state",
            json!({"enabled":true,"persistence":"source_file"}),
        ),
        (
            "endpoints.set_directions",
            json!({"directions":{"inbound":true,"outbound":true},"persistence":"source_file"}),
        ),
        ("endpoints.restart", json!({})),
        ("endpoints.clear_overrides", json!({})),
    ] {
        let mut params = extra;
        params["endpoint_id"] = json!(current.endpoint_id);
        params["expected_core_instance_id"] = json!(stale.core_instance_id);
        params["expected_intent_revision"] = json!(stale.intent_revision);
        let command: CommandRequest =
            serde_json::from_value(json!({"method":method,"params":params})).unwrap();
        let error = handle.execute_acknowledged(command).await.unwrap_err();
        assert_eq!(error.code, ApiErrorCode::Conflict);
        assert_eq!(
            error.field_path.as_deref(),
            Some("params.expected_core_instance_id")
        );
        let after = endpoint(&handle, "a");
        assert_eq!(after.intent_revision, current.intent_revision);
        assert_eq!(after.generation, current.generation);
        assert_eq!(after.state, EndpointRuntimeState::Running);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_instance_and_intent_allow_control_and_stale_intent_still_conflicts() {
    let proxy = Proxy::new(config(free_udp_port(), free_udp_port())).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    let current = endpoint(&handle, "a");
    let command: CommandRequest = serde_json::from_value(json!({"method":"endpoints.set_state","params":{
        "endpoint_id":current.endpoint_id,"enabled":false,"expected_core_instance_id":current.core_instance_id,
        "expected_intent_revision":current.intent_revision}})).unwrap();
    let response = handle.execute_acknowledged(command.clone()).await.unwrap();
    assert_eq!(
        response.result.unwrap()["endpoint"]["core_instance_id"],
        current.core_instance_id
    );
    let stopped = endpoint(&handle, "a");
    assert_eq!(stopped.state, EndpointRuntimeState::Stopped);
    let error = handle.execute_acknowledged(command).await.unwrap_err();
    assert_eq!(error.code, ApiErrorCode::Conflict);
    assert_eq!(
        error.field_path.as_deref(),
        Some("params.expected_intent_revision")
    );
    assert_eq!(
        endpoint(&handle, "a").intent_revision,
        stopped.intent_revision
    );
    handle
        .execute_acknowledged(management::set_state("a", true))
        .await
        .unwrap();
    assert_eq!(
        endpoint(&handle, "a").state,
        EndpointRuntimeState::Running,
        "legacy commands without optional conditions remain compatible"
    );
    running.shutdown().await.unwrap();
}

#[test]
fn resource_operations_publish_precise_persistence_and_live_contraction_limits() {
    let initial = config(free_udp_port(), free_udp_port());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let proxy =
        Proxy::from_engine(Engine::new_with_config_path(initial.clone(), &path).unwrap()).unwrap();
    let endpoint = endpoint(&handle(&proxy), "a");
    let capabilities = &endpoint.supported.operation_capabilities;
    for operation in ["set_state", "set_directions"] {
        assert_eq!(
            capabilities[operation].persistence,
            [
                EndpointPersistence::RuntimeOnly,
                EndpointPersistence::SourceFile
            ]
        );
        assert_eq!(
            capabilities[operation].preconditions,
            ["expected_core_instance_id", "expected_intent_revision"]
        );
    }
    for operation in ["restart", "clear_overrides"] {
        assert_eq!(
            capabilities[operation].persistence,
            [EndpointPersistence::RuntimeOnly]
        );
    }
    let live = capabilities["set_directions"]
        .live_direction_contraction
        .unwrap();
    assert!(live.inbound);
    assert!(!live.outbound);
    assert_eq!(
        capabilities["clear_overrides"].live_direction_contraction,
        Some(live)
    );
    assert!(!endpoint
        .supported
        .operations
        .iter()
        .any(|operation| operation.contains("packet_route")));
    assert!(endpoint.counters.inner_rx_bytes.is_none());
    assert!(endpoint.counters.active_packet_routes.is_none());

    let mut legacy = initial;
    legacy.endpoints.clear();
    let proxy = Proxy::from_engine(Engine::new_with_config_path(legacy, &path).unwrap()).unwrap();
    let zero_api::QueryResponse::Endpoints(list) = zero_api::QueryService::query(
        &handle(&proxy),
        zero_api::QueryRequest::Endpoints(Default::default()),
    )
    .unwrap() else {
        panic!("wrong response")
    };
    for endpoint in list.endpoints {
        assert_eq!(
            endpoint.configuration.origin,
            zero_api::EndpointConfigurationOrigin::Legacy
        );
        assert!(!endpoint.configuration.source_file.available);
        assert_eq!(
            endpoint.supported.operation_capabilities["set_state"].persistence,
            [EndpointPersistence::RuntimeOnly]
        );
    }
}
