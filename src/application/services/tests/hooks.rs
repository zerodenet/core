//! The production ConfigApplyReconciler owns the same concrete hook originals.
use super::*;
use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixListener,
    sync::Arc,
};
use zero_engine::{FlowContext, FlowHook};
fn context() -> FlowContext {
    FlowContext {
        flow_id: 3,
        inbound_tag: Some("direct".into()),
        outbound_tag: None,
        target_host: "example.test".into(),
        target_port: 443,
        network: "tcp".into(),
        protocol: "direct".into(),
        auth: None,
        mode: "rule".into(),
        started_at_unix_ms: 1,
        labels: Vec::new(),
    }
}
fn peer(path: &Path, count: usize, allow: bool) -> std::thread::JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    std::thread::spawn(move || {
        for _ in 0..count {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream).read_line(&mut line).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&line).unwrap()["type"],
                "check_flow"
            );
            stream
                .write_all(if allow {
                    b"{\"allow\":true}\n"
                } else {
                    b"{\"allow\":false}\n"
                })
                .unwrap();
            assert_eq!(stream.read(&mut [0]).unwrap(), 0);
        }
    })
}
async fn original(services: &ApplicationServices) -> Arc<crate::hooks::ipc::IpcFlowHook> {
    services
        .state
        .lock()
        .await
        .hooks
        .as_ref()
        .unwrap()
        .originals_for_test()[0]
        .clone()
}
#[tokio::test]
async fn hook_change_commits_after_old_native_close_and_failed_app_restores_a_new_original() {
    let directory = tempfile::tempdir().unwrap();
    let old_path = directory.path().join("old.sock");
    let next_path = directory.path().join("next.sock");
    let old_peer = peer(&old_path, 1, false);
    let next_peer = peer(&next_path, 2, true);
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json("events.jsonl", 0)).unwrap();
    config["api"]["hooks"] = serde_json::json!([{"type":"ipc","socket":old_path,"timeout_ms":100}]);
    let source_path = directory.path().join("config.json");
    std::fs::write(&source_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let engine = Engine::from_path(&source_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone())
        .with_config_apply_reconciler(services.clone());
    let running = proxy.spawn();
    let old = original(&services).await;
    assert!(old.on_flow_start(&context()).is_err());
    assert_eq!(old.native_observation_for_test(), (1, 0, false, false));
    old_peer.join().unwrap();
    config["api"]["hooks"][0]["socket"] = serde_json::json!(next_path);
    handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: config.clone(),
        }))
        .await
        .unwrap();
    assert!(old.is_retired());
    let next = original(&services).await;
    assert!(!Arc::ptr_eq(&old, &next));
    assert_eq!(next.native_observation_for_test(), (0, 0, false, false));
    next.on_flow_start(&context()).unwrap();
    assert_eq!(next.native_observation_for_test(), (1, 0, false, false));
    let committed = engine.runtime_snapshot();
    let committed_bytes = std::fs::read(&source_path).unwrap();
    let blocked = directory.path().join("not-a-sink-file");
    std::fs::create_dir(&blocked).unwrap();
    let mut failed = config;
    failed["api"]["hooks"][0]["socket"] = serde_json::json!(old_path);
    failed["api"]["event_sinks"][0]["path"] = serde_json::json!(blocked);
    let error = handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: failed,
        }))
        .await
        .unwrap_err();
    assert!(
        error.message.contains("restored last-known-good"),
        "{}",
        error.message
    );
    assert!(Arc::ptr_eq(&committed, &engine.runtime_snapshot()));
    assert_eq!(std::fs::read(&source_path).unwrap(), committed_bytes);
    assert!(next.is_retired());
    assert_eq!(next.native_observation_for_test(), (1, 0, false, false));
    let recovered = original(&services).await;
    assert!(!Arc::ptr_eq(&next, &recovered));
    recovered.on_flow_start(&context()).unwrap();
    assert_eq!(
        recovered.native_observation_for_test(),
        (1, 0, false, false)
    );
    next_peer.join().unwrap();
    running.shutdown().await.unwrap();
    services.shutdown_status_monitor().await.unwrap();
    let retired = Arc::downgrade(&recovered);
    drop(recovered);
    services.shutdown_dispatcher().await.unwrap();
    assert!(
        retired.upgrade().is_none(),
        "final application chain must release its warning-handler Engine cycle"
    );
}

#[tokio::test]
async fn interrupted_actual_hook_original_is_retained_and_blocks_app_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line).unwrap();
        assert_eq!(line, "actual original pending callback\n");
        assert_eq!(stream.read(&mut [0]).unwrap(), 0); // only after explicit cleanup below
    });
    let mut value: serde_json::Value =
        serde_json::from_str(&config_json("events.jsonl", 0)).unwrap();
    value["api"]["hooks"] = serde_json::json!([{"type":"ipc","socket":path,"timeout_ms":100}]);
    let config =
        RuntimeConfig::parse_with_source_dir(&value.to_string(), Some(directory.path().to_owned()))
            .unwrap();
    let engine = Engine::new(config.clone()).unwrap();
    let services = ApplicationServices::start(engine, None).await.unwrap();
    let original = original(&services).await;
    let callback_original = original.clone();
    let interrupted =
        tokio::task::spawn_blocking(move || callback_original.interrupt_original_for_test());
    assert!(matches!(interrupted.await,Err(error) if error.is_panic()));
    assert_eq!(original.native_observation_for_test(), (0, 0, true, true));
    let mut candidate = config.clone();
    candidate.api.hooks.clear();
    use zero_proxy::ConfigApplyReconciler;
    assert!(services.validate(&config, &candidate).is_err());
    assert!(services.reconcile(Arc::new(candidate)).await.is_err());
    assert_eq!(original.native_observation_for_test(), (0, 0, true, true));
    services.shutdown_status_monitor().await.unwrap();
    assert!(services.shutdown_dispatcher().await.is_err());
    assert_eq!(original.native_observation_for_test(), (1, 0, false, true));
    let retained = services
        .state
        .lock()
        .await
        .hooks
        .as_ref()
        .unwrap()
        .originals_for_test()[0]
        .clone();
    assert!(Arc::ptr_eq(&original, &retained));
    assert!(services.state.lock().await.cleanup_fault.is_some());
    server.join().unwrap();
}
