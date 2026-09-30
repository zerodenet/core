#![cfg(feature = "wireguard")]

#[path = "endpoint_management/support.rs"]
mod management;
mod support;

use management::{config, endpoint, handle, ready, set_state};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use support::{free_udp_port, spawn_engine};
use zero_api::{CommandRequest, CommandService, EndpointPersistence, EndpointRuntimeState};
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult, Proxy};

fn persist_stop(current: &zero_api::EndpointSnapshot) -> CommandRequest {
    let CommandRequest::EndpointSetState(mut command) = set_state("a", false) else {
        unreachable!()
    };
    command.persistence = EndpointPersistence::SourceFile;
    command.expected_core_instance_id = Some(current.core_instance_id.clone());
    command.expected_intent_revision = Some(current.intent_revision);
    CommandRequest::EndpointSetState(command)
}

#[tokio::test]
async fn failed_first_source_write_preserves_running_resources_and_can_retry() {
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("source");
    std::fs::write(&parent, "parent is a file").unwrap();
    let path = parent.join("config.json");
    let proxy = Proxy::from_engine(
        Engine::new_with_config_path(config(free_udp_port(), free_udp_port()), &path).unwrap(),
    )
    .unwrap();
    let controlled = handle(&proxy);
    let running = spawn_engine(proxy);
    ready(&controlled).await;
    let before = endpoint(&controlled, "a");
    let command = persist_stop(&before);
    let error = controlled
        .execute_acknowledged(command.clone())
        .await
        .unwrap_err();
    assert!(error.message.contains("restored previous"), "{error:?}");
    let restored = endpoint(&controlled, "a");
    assert_eq!(restored.state, EndpointRuntimeState::Running);
    assert_eq!(restored.intent_revision, before.intent_revision);
    assert_eq!(restored.generation, before.generation);
    assert!(restored.effective.outbound);
    assert!(restored.last_error.is_some());
    assert_eq!(
        endpoint(&controlled, "b").state,
        EndpointRuntimeState::Running
    );
    assert_eq!(
        std::fs::read_to_string(&parent).unwrap(),
        "parent is a file"
    );
    assert_eq!(restored.configuration.source_file.writable, None);
    assert_eq!(
        restored.configuration.source_file.reason.as_deref(),
        Some("source_write_failed")
    );
    assert!(restored
        .configuration
        .source_file
        .writable_observed_at_unix_ms
        .is_some());
    std::fs::remove_file(&parent).unwrap();
    std::fs::create_dir(&parent).unwrap();
    controlled.execute_acknowledged(command).await.unwrap();
    let stopped = endpoint(&controlled, "a");
    assert_eq!(stopped.state, EndpointRuntimeState::Stopped);
    assert_eq!(stopped.configuration.source_file.writable, Some(true));
    assert!(stopped.configuration.source_file.reason.is_none());
    assert!(!RuntimeConfig::load_from_path(&path).unwrap().endpoints[0].enabled);
    assert_eq!(
        endpoint(&controlled, "b").state,
        EndpointRuntimeState::Running
    );
    running.shutdown().await.unwrap();
}

#[derive(Default)]
struct FailOnceReconciler(AtomicBool);

#[async_trait::async_trait]
impl ConfigApplyReconciler for FailOnceReconciler {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }

    async fn reconcile(&self, _: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        if !self.0.swap(true, Ordering::SeqCst) {
            Err("injected reconciliation failure after persistence".into())
        } else {
            Ok(ConfigReconcileResult::default())
        }
    }
}

#[tokio::test]
async fn failure_after_successful_source_write_restores_source_and_resources() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    let initial = config(free_udp_port(), free_udp_port());
    std::fs::write(&path, serde_json::to_vec(&initial).unwrap()).unwrap();
    let proxy = Proxy::from_engine(Engine::new_with_config_path(initial, &path).unwrap()).unwrap();
    let plain = handle(&proxy);
    let controlled = plain
        .clone()
        .with_config_apply_reconciler(Arc::new(FailOnceReconciler::default()));
    let running = spawn_engine(proxy.clone());
    ready(&plain).await;
    let before = endpoint(&plain, "a");
    let error = controlled
        .execute_acknowledged(persist_stop(&before))
        .await
        .unwrap_err();
    assert!(error.message.contains("injected reconciliation failure"));
    assert!(error.message.contains("restored previous"), "{error:?}");
    let restored = endpoint(&plain, "a");
    assert_eq!(restored.state, EndpointRuntimeState::Running);
    assert_eq!(restored.intent_revision, before.intent_revision);
    assert!(RuntimeConfig::load_from_path(&path).unwrap().endpoints[0].enabled);
    assert_eq!(proxy.engine().config_source_write_generation(), 2);
    assert_eq!(restored.configuration.source_file.writable, Some(true));
    assert_eq!(endpoint(&plain, "b").state, EndpointRuntimeState::Running);
    running.shutdown().await.unwrap();
}
