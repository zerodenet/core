use crate::runtime::{orchestration::state::OrchestrationState, Proxy};
use std::sync::Arc;
use zero_config::RuntimeConfig;

#[tokio::test]
async fn direct_engine_notifications_survive_the_actual_host_startup_snapshot_window() {
    let original = RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(original).unwrap();
    let mut state = OrchestrationState::assemble(&proxy).unwrap();
    let before = state.applied_snapshot.clone();
    let candidate = RuntimeConfig::parse(r#"{"route":{"final":{"type":"reject"}}}"#).unwrap();
    proxy.engine().reload_runtime_config(candidate).unwrap();
    for _ in 0..64 {
        proxy.engine().notify_configuration_reconcile();
    }
    assert!(state.reload_changes.has_changed().unwrap());
    state.start(&proxy).await.unwrap();
    assert!(Arc::ptr_eq(&state.applied_snapshot, &before));
    // changed(), rather than a bridge task or a sleep, observes the domain
    // change retained since assemble's subscribe-before-capture boundary.
    state.reload_changes.changed().await.unwrap();
    state.reconcile_reload(&proxy).await;
    assert!(Arc::ptr_eq(
        &state.applied_snapshot,
        &proxy.engine().runtime_snapshot()
    ));
    state.finish(&proxy, Ok(())).await.unwrap();
    assert!(proxy.host.apply().is_closing());
}
