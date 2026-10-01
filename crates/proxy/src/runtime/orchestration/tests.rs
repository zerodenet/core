use zero_engine::EngineError;

use super::lifecycle::{
    handle_configured_tun_failure, handle_listener_result, handle_urltest_result,
};

#[test]
fn unexpected_clean_listener_exit_is_fatal() {
    let mut expected_exits = 0;
    let result = handle_listener_result(Some(Ok(Ok(()))), false, &mut expected_exits);

    assert!(matches!(result, Err(EngineError::InboundTaskExited)));
}

#[test]
fn expected_listener_exit_is_consumed_during_reconciliation() {
    let mut expected_exits = 1;
    let result = handle_listener_result(Some(Ok(Ok(()))), false, &mut expected_exits);

    assert!(result.is_ok());
    assert_eq!(expected_exits, 0);
}

#[test]
fn listener_error_is_preserved_during_reconciliation() {
    let mut expected_exits = 1;
    let result = handle_listener_result(
        Some(Ok(Err(EngineError::NoInbounds))),
        false,
        &mut expected_exits,
    );

    assert!(matches!(result, Err(EngineError::NoInbounds)));
    assert_eq!(expected_exits, 1);
}

#[test]
fn clean_listener_exit_is_allowed_during_shutdown() {
    let mut expected_exits = 0;
    let result = handle_listener_result(Some(Ok(Ok(()))), true, &mut expected_exits);

    assert!(result.is_ok());
}

#[test]
fn unexpected_clean_urltest_exit_is_fatal() {
    let result = handle_urltest_result(Some(Ok(Ok(()))), false);

    assert!(matches!(result, Err(EngineError::UrlTestTaskExited)));
}

#[test]
fn clean_urltest_exit_is_allowed_during_shutdown() {
    let result = handle_urltest_result(Some(Ok(Ok(()))), true);

    assert!(result.is_ok());
}

#[test]
fn configured_tun_failure_is_fatal_to_orchestration() {
    let error = handle_configured_tun_failure(Ok("device read failed".to_owned()))
        .expect_err("configured TUN failure must fail the runtime");

    assert!(error.to_string().contains("configured TUN runtime failed"));
}

#[tokio::test]
async fn duplicate_reload_notification_preserves_dns_candidate_owned_by_acknowledged_apply() {
    use crate::runtime::{PendingReloadAck, Proxy};
    use zero_config::RuntimeConfig;

    let initial =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(initial.clone()).unwrap();
    let mut state = super::state::OrchestrationState::new(&proxy).await.unwrap();
    let previous = proxy.engine.runtime_snapshot();
    let mut candidate = initial;
    candidate.runtime.event_log_capacity += 1;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    proxy.engine.stage_runtime_config(candidate).unwrap();
    *proxy.reload_ack.lock().unwrap() = Some(PendingReloadAck {
        expected: proxy.engine.runtime_snapshot(),
        previous,
        persist: false,
        sender,
    });
    state.reconcile_reload(&proxy).await;
    receiver.await.unwrap().unwrap();
    // Deterministically replay a late rollback/reload notification in the
    // interval before the apply owner commits its prepared DNS state.
    state.reconcile_reload(&proxy).await;
    proxy
        .resolver
        .commit_prepared_reload()
        .expect("only the apply owner commits DNS");
}

#[tokio::test]
async fn identical_config_snapshots_do_not_cross_acknowledge_reload_operations() {
    use crate::runtime::{PendingReloadAck, Proxy};
    use zero_config::RuntimeConfig;
    let config =
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let previous = proxy.engine.runtime_snapshot();
    proxy.engine.stage_runtime_config(config).unwrap();
    let candidate = proxy.engine.runtime_snapshot();
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    *proxy.reload_ack.lock().unwrap() = Some(PendingReloadAck {
        expected: candidate.clone(),
        previous: previous.clone(),
        persist: false,
        sender,
    });
    proxy.complete_reload(&previous, Ok(()));
    assert_eq!(
        receiver.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    );
    proxy.complete_reload(&candidate, Ok(()));
    receiver.await.unwrap().unwrap();
}

#[tokio::test]
async fn duplicate_reload_wakeup_preserves_dns_prepared_for_transaction_commit() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{"inbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let mut state = super::state::OrchestrationState::new(&proxy).await.unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    proxy
        .resolver
        .prepare_reload_with_dispatch(
            snapshot.config().runtime.dns.as_ref(),
            snapshot.config().compile_dns_dispatch().unwrap(),
        )
        .unwrap();

    // The previous reconcile has acknowledged this snapshot, but its caller
    // has not yet committed DNS. Queued rollback/apply wakeups must be inert.
    state.reconcile_reload(&proxy).await;
    state.reconcile_reload(&proxy).await;

    assert!(std::sync::Arc::ptr_eq(
        &snapshot,
        &proxy.engine().runtime_snapshot()
    ));
    assert_eq!(proxy.engine().config_revision(), 1);
    proxy
        .resolver
        .commit_prepared_reload()
        .expect("duplicate notifications must preserve the prepared DNS candidate");
}
