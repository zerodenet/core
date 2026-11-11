use super::*;
use std::sync::Arc;

#[tokio::test]
async fn hosted_quota_final_flush_uses_the_adopted_source_and_preserves_balances() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quota.json");
    let mut config = engine().config().as_ref().clone();
    config.runtime.principal_quota_state_path = Some(path.display().to_string());
    let engine = Engine::new(config.clone()).unwrap();
    let source = engine.configuration_source_owner();
    let memory = source.quota_memory(&config.runtime).unwrap();
    let roots = zero_runtime::HostOriginalBudgets::default();
    source.adopt_quota_memory(&roots).unwrap();
    let (id, mut session) = admit(&engine, 1, 100).unwrap();
    engine.record_session_inbound_rx(id, 31);
    assert_eq!(
        engine
            .shutdown_principal_quota_state_async()
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(!engine.principal_quota_retirement_complete());
    session.finish(SessionOutcome::DirectRelayed).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        engine.shutdown_principal_quota_state_async(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(engine.principal_quota_retirement_complete());
    assert_eq!(roots.component_tasks.usage().charged, 0);
    assert!(memory.same_budget(&source.quota_memory(&config.runtime).unwrap()));
    source.confirm_quota_storage_closed().unwrap();
    source.confirm_original_io_closed().unwrap();
    let restored = Engine::new(config).unwrap();
    let (id, mut session) = admit(&restored, 1, 100).unwrap();
    restored.record_session_inbound_tx(id, 69);
    assert!(
        session.is_cancelled(),
        "same final quota balance, not a fresh book"
    );
    session.finish(SessionOutcome::Cancelled).unwrap();
    restored.shutdown_principal_quota_state().unwrap();
}

#[test]
fn quota_memory_publication_rejects_live_shrink_without_changing_the_snapshot_or_evicting_balances()
{
    let engine = engine();
    let source = engine.configuration_source_owner();
    let memory = source.quota_memory(&engine.config().runtime).unwrap();
    let (_, mut session) = admit(&engine, 1, 100).unwrap();
    session.finish(SessionOutcome::DirectRelayed).unwrap();
    let original = engine.runtime_snapshot();
    let previous = memory.snapshot();
    let mut config = original.config().as_ref().clone();
    config.runtime.principal_quota_total_max_storage_bytes = Some(1);
    assert!(engine.reload_runtime_config(config).is_err());
    assert!(Arc::ptr_eq(&engine.runtime_snapshot(), &original));
    assert_eq!(memory.snapshot().limit, previous.limit);
    let mut config = original.config().as_ref().clone();
    config.runtime.principal_quota_total_max_storage_bytes = Some(8 * 1024 * 1024);
    engine.reload_runtime_config(config).unwrap();
    assert_eq!(memory.snapshot().limit.bytes(), 8 * 1024 * 1024);
    assert!(memory.same_budget(&source.quota_memory(&original.config().runtime).unwrap()));
    // Reading through the historical config cannot restore its older ceiling.
    assert_eq!(memory.snapshot().limit.bytes(), 8 * 1024 * 1024);
    let (_, mut next) = admit(&engine, 1, 100).unwrap();
    next.finish(SessionOutcome::DirectRelayed).unwrap();
}
