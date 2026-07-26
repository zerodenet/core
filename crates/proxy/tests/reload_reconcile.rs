use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use zero_api::{CommandRequest, CommandService, ConfigApplyCommand};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult, Proxy, ProxyHandle};

struct RecordingReconciler {
    calls: Mutex<Vec<u16>>,
    fail_port: Option<u16>,
}

#[async_trait::async_trait]
impl ConfigApplyReconciler for RecordingReconciler {
    fn validate(&self, _current: &RuntimeConfig, _candidate: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }

    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        let port = target.inbounds[0].listen.port;
        self.calls.lock().unwrap().push(port);
        if self.fail_port == Some(port) {
            return Err("injected application service failure".to_owned());
        }
        Ok(ConfigReconcileResult {
            components: vec!["test-service".to_owned()],
        })
    }
}

fn config(port: u16) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{
                "tag":"managed-in",
                "listen":{{"address":"127.0.0.1","port":{port}}},
                "protocol":{{"type":"direct"}}
            }}],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#
    ))
    .unwrap()
}

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for_connect(port: u16) {
    tokio::time::timeout(Duration::from_secs(2), async move {
        loop {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener did not become reachable");
}

fn vless_config(port: u16, websocket: bool) -> RuntimeConfig {
    let ws = if websocket {
        r#", "ws":{"path":"/panel","headers":{}}"#
    } else {
        ""
    };
    RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{
                "tag":"managed-vless",
                "listen":{{"address":"127.0.0.1","port":{port}}},
                "protocol":{{
                    "type":"vless",
                    "users":[{{"id":"11111111-1111-1111-1111-111111111111"}}]
                    {ws}
                }}
            }}],
            "outbounds":[{{"tag":"direct","protocol":{{"type":"direct"}}}}],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#
    ))
    .unwrap()
}

#[tokio::test]
async fn acknowledged_reload_restarts_changed_same_tag_listener() {
    let old_port = free_port().await;
    let new_port = free_port().await;
    let proxy = Proxy::new(config(old_port)).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    handle
        .apply_config_and_wait(config(new_port), Duration::from_secs(5))
        .await
        .expect("listener reconcile acknowledgement");
    wait_for_connect(new_port).await;
    assert!(TcpStream::connect(("127.0.0.1", old_port)).await.is_err());

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_only_reload_does_not_replace_operator_config_file() {
    let old_port = free_port().await;
    let new_port = free_port().await;
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.json");
    let original = config(old_port);
    std::fs::write(&config_path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(original, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    handle
        .apply_runtime_config_and_wait(config(new_port), Duration::from_secs(5))
        .await
        .expect("runtime-only listener reconcile acknowledgement");

    wait_for_connect(new_port).await;
    assert_eq!(engine.config().inbounds[0].listen.port, new_port);
    let persisted = RuntimeConfig::load_from_path(&config_path).unwrap();
    assert_eq!(persisted.inbounds[0].listen.port, old_port);

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_runtime_only_reload_rolls_back_without_replacing_operator_config_file() {
    let old_port = free_port().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.json");
    let original = config(old_port);
    std::fs::write(&config_path, serde_json::to_vec_pretty(&original).unwrap()).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(original, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    handle
        .apply_runtime_config_and_wait(config(occupied_port), Duration::from_secs(5))
        .await
        .expect_err("occupied runtime-only listener must reject reload");

    assert_eq!(engine.config().inbounds[0].listen.port, old_port);
    let persisted = RuntimeConfig::load_from_path(&config_path).unwrap();
    assert_eq!(persisted.inbounds[0].listen.port, old_port);
    wait_for_connect(old_port).await;

    drop(occupied);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn acknowledged_reload_rebinds_changed_transport_on_same_endpoint() {
    let port = free_port().await;
    let proxy = Proxy::new(vless_config(port, false)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(port).await;

    handle
        .apply_config_and_wait(vless_config(port, true), Duration::from_secs(5))
        .await
        .expect("same-endpoint listener reconcile acknowledgement");
    wait_for_connect(port).await;
    let zero_config::InboundProtocolConfig::Vless { ws, .. } =
        &engine.config().inbounds[0].protocol
    else {
        panic!("expected VLESS");
    };
    assert_eq!(ws.as_deref().unwrap().path, "/panel");

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_listener_rebind_restores_last_known_good_config() {
    let old_port = free_port().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let proxy = Proxy::new(config(old_port)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    let error = handle
        .apply_config_and_wait(config(occupied_port), Duration::from_secs(5))
        .await
        .expect_err("occupied listener must reject acknowledged reload");
    assert!(!error.is_empty());
    assert_eq!(engine.config().inbounds[0].listen.port, old_port);
    wait_for_connect(old_port).await;

    drop(occupied);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn timed_out_reload_waits_for_last_known_good_rollback() {
    let old_port = free_port().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let proxy = Proxy::new(config(old_port)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    let error = handle
        .apply_config_and_wait(config(occupied_port), Duration::from_millis(10))
        .await
        .expect_err("short apply timeout must fail and roll back");
    assert!(error.contains("restored last-known-good config"), "{error}");
    assert_eq!(engine.config().inbounds[0].listen.port, old_port);
    wait_for_connect(old_port).await;

    drop(occupied);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn runtime_config_principal_impact_is_enforced_after_reconciliation() {
    let port = free_port().await;
    let proxy = Proxy::new(config(port)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(port).await;

    let (cancelled_tx, mut cancelled_rx) = tokio::sync::mpsc::unbounded_channel();
    let _registration = engine.register_principal_cancellation("account:old", move |reason| {
        let _ = cancelled_tx.send(reason);
    });
    handle
        .apply_runtime_config_with_principal_impact_and_wait(
            config(port),
            vec!["account:old".to_owned()],
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("acknowledged user apply");
    assert_eq!(
        cancelled_rx.recv().await.as_deref(),
        Some("principal_disabled")
    );

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_runtime_overlay_cannot_overwrite_a_newer_local_config_apply() {
    let original_port = free_port().await;
    let local_port = free_port().await;
    let proxy = Proxy::new(config(original_port)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(original_port).await;

    let connector_snapshot = (*engine.config()).clone();
    handle
        .apply_config_and_wait(config(local_port), Duration::from_secs(5))
        .await
        .expect("local config apply");
    wait_for_connect(local_port).await;

    let applied = handle
        .apply_runtime_config_if_current_with_principal_impact_and_wait(
            &connector_snapshot,
            connector_snapshot.clone(),
            Vec::new(),
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .expect("conditional connector apply");
    assert!(!applied, "stale runtime overlay must be rejected");
    assert_eq!(engine.config().inbounds[0].listen.port, local_port);
    wait_for_connect(local_port).await;

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_runtime_config_does_not_enforce_principal_impact() {
    let old_port = free_port().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = occupied.local_addr().unwrap().port();
    let proxy = Proxy::new(config(old_port)).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    let (cancelled_tx, mut cancelled_rx) = tokio::sync::mpsc::unbounded_channel();
    let _registration = engine.register_principal_cancellation("account:old", move |reason| {
        let _ = cancelled_tx.send(reason);
    });
    handle
        .apply_runtime_config_with_principal_impact_and_wait(
            config(occupied_port),
            vec!["account:old".to_owned()],
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .expect_err("occupied listener must reject user apply");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), cancelled_rx.recv())
            .await
            .is_err(),
        "rolled-back user apply revoked a last-known-good principal"
    );
    assert_eq!(engine.config().inbounds[0].listen.port, old_port);
    wait_for_connect(old_port).await;

    drop(occupied);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn config_apply_command_waits_for_proxy_and_application_reconciliation() {
    let old_port = free_port().await;
    let new_port = free_port().await;
    let proxy = Proxy::new(config(old_port)).unwrap();
    let engine = proxy.engine().clone();
    let reconciler = Arc::new(RecordingReconciler {
        calls: Mutex::new(Vec::new()),
        fail_port: None,
    });
    let handle = ProxyHandle::new(EngineHandle::new(engine), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    let response = handle
        .execute_acknowledged(CommandRequest::ConfigApply(ConfigApplyCommand {
            config: serde_json::to_value(config(new_port)).unwrap(),
        }))
        .await
        .expect("acknowledged config.apply");

    assert_eq!(
        response.result.as_ref().unwrap()["persistence"],
        "source_file"
    );
    assert_eq!(
        response.result.as_ref().unwrap()["application_components"],
        serde_json::json!(["test-service"])
    );
    assert_eq!(*reconciler.calls.lock().unwrap(), vec![new_port]);
    wait_for_connect(new_port).await;
    assert!(TcpStream::connect(("127.0.0.1", old_port)).await.is_err());

    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn application_reconcile_failure_restores_proxy_file_and_services() {
    let old_port = free_port().await;
    let new_port = free_port().await;
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.json");
    std::fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config(old_port)).unwrap(),
    )
    .unwrap();
    let original = RuntimeConfig::load_from_path(&config_path).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(original, &config_path).unwrap();
    let proxy = Proxy::from_engine(engine.clone()).unwrap();
    let reconciler = Arc::new(RecordingReconciler {
        calls: Mutex::new(Vec::new()),
        fail_port: Some(new_port),
    });
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    wait_for_connect(old_port).await;

    let error = handle
        .apply_config_and_wait(config(new_port), Duration::from_secs(5))
        .await
        .expect_err("application reconcile failure must reject the transaction");

    assert!(
        error.contains("restored last-known-good configuration"),
        "{error}"
    );
    assert_eq!(engine.config().inbounds[0].listen.port, old_port);
    assert_eq!(
        RuntimeConfig::load_from_path(&config_path)
            .unwrap()
            .inbounds[0]
            .listen
            .port,
        old_port
    );
    assert_eq!(*reconciler.calls.lock().unwrap(), vec![new_port, old_port]);
    wait_for_connect(old_port).await;
    assert!(TcpStream::connect(("127.0.0.1", new_port)).await.is_err());

    running.shutdown().await.unwrap();
}
