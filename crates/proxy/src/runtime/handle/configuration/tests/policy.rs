use super::owned::paused;
use super::*;
use zero_api::{CommandRequest, CommandService, ModeSetCommand, PolicySelectCommand};

fn selected(proxy: &Proxy) -> String {
    let snapshot = proxy.engine().runtime_snapshot();
    let id = snapshot.plan().target_id("pick").unwrap();
    let selection = snapshot.group_selection_binding(id).unwrap();
    snapshot
        .plan()
        .target(selection.selected())
        .unwrap()
        .tag()
        .to_owned()
}
fn selector_config(cidr: &str, reverse: bool) -> RuntimeConfig {
    let mut config = config(cidr);
    let nodes = if reverse { ["b", "a"] } else { ["a", "b"] };
    config.outbounds = serde_json::from_value(serde_json::json!(
        nodes.map(|tag| serde_json::json!({"tag":tag,"protocol":{"type":"direct"}}))
    ))
    .unwrap();
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"pick","type":"selector","outbounds":["a","b"],"selected":"a"}
    ]))
    .unwrap();
    config
}
#[tokio::test]
async fn selector_ack_during_actual_pending_transaction_survives_plan_id_remapping() {
    let proxy = Proxy::new(selector_config("198.18.0.0/24", false)).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let apply = handle.clone();
    let running = proxy.spawn();
    let request = tokio::spawn(async move {
        apply
            .apply_runtime_config_and_wait(
                selector_config("198.19.0.0/24", true),
                Duration::from_secs(5),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert_eq!(selected(&proxy), "a");
    let ack = handle
        .execute(CommandRequest::PolicySelect(PolicySelectCommand {
            policy_tag: "pick".into(),
            target_tag: "b".into(),
        }))
        .unwrap();
    assert!(ack.accepted);
    assert_eq!(selected(&proxy), "b");
    reconciler.release.notify_one();
    request.await.unwrap().unwrap();
    assert_eq!(selected(&proxy), "b");
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn mode_ack_during_pending_configuration_conflicts_without_rolling_back_its_revision() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let apply = handle.clone();
    let running = proxy.spawn();
    let request = tokio::spawn(async move {
        apply
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    let before = proxy.engine().config_revision();
    let ack = handle
        .execute(CommandRequest::ModeSet(ModeSetCommand {
            mode: "direct".into(),
            outbound: None,
        }))
        .unwrap();
    assert!(ack.accepted);
    let committed_mode = proxy.engine().runtime_snapshot();
    assert_eq!(committed_mode.config_revision(), before + 1);
    reconciler.release.notify_one();
    let error = request.await.unwrap().unwrap_err();
    assert!(
        error.contains("cannot publish a snapshot across a committed configuration change"),
        "{error}"
    );
    assert!(Arc::ptr_eq(
        &proxy.engine().runtime_snapshot(),
        &committed_mode
    ));
    assert_eq!(proxy.engine().config_revision(), before + 1);
    assert_eq!(proxy.engine().mode_kind(), "direct");
    running.shutdown().await.unwrap();
}
