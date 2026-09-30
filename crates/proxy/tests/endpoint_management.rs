#![cfg(feature = "wireguard")]

#[path = "endpoint_management/support.rs"]
mod management;
mod support;

use management::{config, endpoint, handle, ready, set_state};
use support::{free_udp_port, spawn_engine};
use tokio::{net::UdpSocket, time::Duration};
use zero_api::{
    event_type, ApiErrorCode, CommandRequest, CommandService, EndpointDirections,
    EndpointOperationCommand, EndpointPersistence, EndpointRuntimeState,
    EndpointSetDirectionsCommand, EndpointSetStateCommand, EndpointStateSource, EventFilter,
};
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::Proxy;

fn operation(tag: &str) -> EndpointOperationCommand {
    EndpointOperationCommand {
        endpoint_id: format!("endpoint:{tag}"),
        expected_intent_revision: None,
        expected_core_instance_id: None,
    }
}

#[tokio::test]
async fn stop_releases_one_listener_reload_retains_override_and_clear_restarts_it() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let initial = config(a, b);
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy.clone());
    ready(&handle).await;
    let started = endpoint(&handle, "a");
    assert!(started.generation.is_some_and(|generation| generation > 0));
    assert!(started.started_at_unix_ms.is_some());
    let result = handle
        .execute_acknowledged(set_state("a", false))
        .await
        .unwrap();
    assert_eq!(result.result.unwrap()["applied"], true);
    let first = endpoint(&handle, "a");
    assert_eq!(first.state, EndpointRuntimeState::Stopped);
    assert_eq!(first.generation, started.generation);
    assert!(first.started_at_unix_ms.is_none());
    assert_eq!(first.state_source, EndpointStateSource::RuntimeOverride);
    assert!(!first.effective.inbound && !first.effective.outbound);
    let released = UdpSocket::bind(("127.0.0.1", a))
        .await
        .expect("stop confirms socket release");
    assert!(UdpSocket::bind(("127.0.0.1", b)).await.is_err());
    handle
        .execute_acknowledged(set_state("a", false))
        .await
        .unwrap();
    assert_eq!(
        first.intent_revision,
        endpoint(&handle, "a").intent_revision
    );
    handle
        .apply_runtime_config_and_wait(initial, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!endpoint(&handle, "a").enabled);
    assert!(proxy.engine().config().endpoints[0].enabled);
    assert_eq!(endpoint(&handle, "b").state, EndpointRuntimeState::Running);
    drop(released);
    handle
        .execute_acknowledged(CommandRequest::EndpointClearOverrides(operation("a")))
        .await
        .unwrap();
    assert_eq!(endpoint(&handle, "a").state, EndpointRuntimeState::Running);
    let resumed = endpoint(&handle, "a");
    assert!(resumed.generation > started.generation);
    assert!(resumed.started_at_unix_ms.is_some());
    let events = proxy.engine().events_snapshot(&EventFilter::default());
    let transitions = events
        .iter()
        .filter(|event| {
            event.event_type == event_type::ENDPOINT_STATE_CHANGED
                && event.payload["endpoint_id"] == "endpoint:a"
        })
        .collect::<Vec<_>>();
    assert!(transitions.len() >= 3);
    assert_eq!(transitions.last().unwrap().payload["state"], "running");
    assert_eq!(
        endpoint(&handle, "a").state_source,
        EndpointStateSource::Config
    );
    assert!(UdpSocket::bind(("127.0.0.1", a)).await.is_err());
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_start_restores_disabled_intent_and_other_endpoint_keeps_running() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let proxy = Proxy::new(config(a, b)).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    handle
        .execute_acknowledged(set_state("a", false))
        .await
        .unwrap();
    let stopped = endpoint(&handle, "a");
    let occupied = UdpSocket::bind(("127.0.0.1", a)).await.unwrap();
    let error = handle
        .execute_acknowledged(set_state("a", true))
        .await
        .unwrap_err();
    assert_eq!(error.code, ApiErrorCode::Internal);
    assert!(
        error.message.contains("restored previous"),
        "{}",
        error.message
    );
    let restored = endpoint(&handle, "a");
    assert!(!restored.enabled);
    assert_eq!(restored.intent_revision, stopped.intent_revision);
    assert_eq!(restored.state, EndpointRuntimeState::Stopped);
    assert!(restored.last_error.is_some());
    assert_eq!(endpoint(&handle, "b").state, EndpointRuntimeState::Running);
    assert_eq!(occupied.local_addr().unwrap().port(), a);
    drop(occupied);
    handle
        .execute_acknowledged(set_state("a", true))
        .await
        .unwrap();
    assert_eq!(endpoint(&handle, "a").state, EndpointRuntimeState::Running);
    assert!(endpoint(&handle, "a").last_error.is_none());
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn config_apply_can_contract_inbound_without_restarting_endpoint() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let proxy = Proxy::new(config(a, b)).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    let mut candidate = config(a, b);
    candidate.endpoints[0].directions = EndpointDirections::outbound_only();
    handle
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
        .await
        .expect("inbound contraction through config.apply");
    let contracted = endpoint(&handle, "a");
    assert_eq!(contracted.state, EndpointRuntimeState::Running);
    assert!(!contracted.effective.inbound && contracted.effective.outbound);
    assert_eq!(contracted.generation, original.generation);
    assert!(UdpSocket::bind(("127.0.0.1", a)).await.is_err());
    assert_eq!(endpoint(&handle, "b").state, EndpointRuntimeState::Running);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_is_explicit_conditional_and_outbound_contraction_requires_stop() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let proxy = Proxy::new(config(a, b)).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    let original = endpoint(&handle, "a");
    handle
        .execute_acknowledged(CommandRequest::EndpointRestart(operation("a")))
        .await
        .unwrap();
    let restarted = endpoint(&handle, "a");
    assert_eq!(restarted.state, EndpointRuntimeState::Running);
    assert!(restarted.intent_revision > original.intent_revision);
    assert_eq!(endpoint(&handle, "b").state, EndpointRuntimeState::Running);
    let error = handle
        .execute_acknowledged(CommandRequest::EndpointSetState(EndpointSetStateCommand {
            expected_intent_revision: Some(original.intent_revision),
            expected_core_instance_id: None,
            ..match set_state("a", false) {
                CommandRequest::EndpointSetState(command) => command,
                _ => unreachable!(),
            }
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code, ApiErrorCode::Conflict);
    let directions = CommandRequest::EndpointSetDirections(EndpointSetDirectionsCommand {
        endpoint_id: "endpoint:a".into(),
        directions: EndpointDirections {
            inbound: true,
            outbound: false,
        },
        persistence: EndpointPersistence::RuntimeOnly,
        expected_intent_revision: None,
        expected_core_instance_id: None,
    });
    assert_eq!(
        handle
            .execute_acknowledged(directions.clone())
            .await
            .unwrap_err()
            .code,
        ApiErrorCode::Unsupported
    );
    assert!(endpoint(&handle, "a").allowed.outbound);
    let mut contraction = config(a, b);
    contraction.endpoints[0].directions = EndpointDirections {
        inbound: true,
        outbound: false,
    };
    let error = handle
        .apply_runtime_config_and_wait(contraction, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        error.contains("live outbound direction contraction"),
        "{error}"
    );
    assert!(
        endpoint(&handle, "a").allowed.outbound,
        "config.apply cannot bypass direction-scope cancellation"
    );
    assert_eq!(endpoint(&handle, "b").state, EndpointRuntimeState::Running);
    handle
        .execute_acknowledged(set_state("a", false))
        .await
        .unwrap();
    assert_eq!(
        handle
            .execute_acknowledged(CommandRequest::EndpointRestart(operation("a")))
            .await
            .unwrap_err()
            .code,
        ApiErrorCode::InvalidArgument
    );
    handle.execute_acknowledged(directions).await.unwrap();
    handle
        .execute_acknowledged(set_state("a", true))
        .await
        .unwrap();
    let next = endpoint(&handle, "a");
    assert!(next.effective.inbound);
    assert!(!next.effective.outbound);
    assert_eq!(next.state, EndpointRuntimeState::Running);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn source_file_control_persists_canonical_config_and_clears_runtime_override() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let initial = config(a, b);
    std::fs::write(&path, serde_json::to_vec(&initial).unwrap()).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let proxy = Proxy::from_engine(Engine::new_with_config_path(initial, &path).unwrap()).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&handle).await;
    handle
        .execute_acknowledged(set_state("a", false))
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    handle
        .execute_acknowledged(CommandRequest::EndpointSetState(EndpointSetStateCommand {
            endpoint_id: "endpoint:a".into(),
            enabled: false,
            persistence: EndpointPersistence::SourceFile,
            expected_intent_revision: None,
            expected_core_instance_id: None,
        }))
        .await
        .unwrap();
    let persisted = RuntimeConfig::load_from_path(&path).unwrap();
    assert!(!persisted.endpoints[0].enabled);
    assert!(persisted.endpoints[1].enabled);
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(raw["endpoints"].as_array().unwrap().len(), 2);
    assert!(!raw["outbounds"]
        .as_array()
        .is_some_and(|values| !values.is_empty()));
    assert!(!raw["inbounds"]
        .as_array()
        .is_some_and(|values| !values.is_empty()));
    assert_eq!(
        endpoint(&handle, "a").state_source,
        EndpointStateSource::Config
    );
    handle
        .execute_acknowledged(CommandRequest::EndpointClearOverrides(operation("a")))
        .await
        .unwrap();
    assert!(!endpoint(&handle, "a").enabled);
    running.shutdown().await.unwrap();
}
