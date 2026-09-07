use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult, Proxy, ProxyHandle};

struct RejectRemovedNode;

#[async_trait::async_trait]
impl ConfigApplyReconciler for RejectRemovedNode {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }

    async fn reconcile(&self, config: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        if config.outbounds.len() == 1 {
            Err("injected application reconciliation failure".to_owned())
        } else {
            Ok(ConfigReconcileResult::default())
        }
    }
}

fn config(port: u16, nodes: &[&str]) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "inbounds": [{"tag": "in", "listen": {"address": "127.0.0.1", "port": port},
            "protocol": {"type": "direct"}}],
        "outbounds": nodes.iter().map(|tag| serde_json::json!({
            "tag": tag, "protocol": {"type": "direct"}
        })).collect::<Vec<_>>(),
        "outbound_groups": [{"tag": "select", "type": "selector", "outbounds": nodes, "selected": "a"}],
        "route": {"rules": [], "final": {"type": "direct"}}
    }).to_string()).unwrap()
}

#[tokio::test]
async fn listener_failure_restores_runtime_selection_of_a_removed_node() {
    assert_rollback(false).await;
}

#[tokio::test]
async fn application_failure_restores_runtime_selection_of_a_removed_node() {
    assert_rollback(true).await;
}

async fn assert_rollback(application_failure: bool) {
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let original_port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate_port = occupied.local_addr().unwrap().port();
    let proxy = Proxy::new(config(original_port, &["a", "b"])).unwrap();
    let engine = proxy.engine().clone();
    engine.set_selector_target("select", "b").unwrap();
    let original = engine.runtime_snapshot();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let handle = if application_failure {
        handle.with_config_apply_reconciler(Arc::new(RejectRemovedNode))
    } else {
        handle
    };
    let running = proxy.spawn();
    if application_failure {
        drop(occupied);
    }
    let error = handle
        .apply_runtime_config_and_wait(config(candidate_port, &["a"]), Duration::from_secs(5))
        .await
        .unwrap_err();
    if application_failure {
        assert!(
            error.contains("injected application reconciliation failure"),
            "{error}"
        );
    }
    assert!(Arc::ptr_eq(&engine.runtime_snapshot(), &original));
    assert_eq!(engine.config_revision(), 1);
    assert_eq!(
        engine.export_status().config.outbound_groups[0]
            .selected
            .as_deref(),
        Some("b")
    );
    assert!(TcpStream::connect(("127.0.0.1", original_port))
        .await
        .is_ok());
    running.shutdown().await.unwrap();
}
