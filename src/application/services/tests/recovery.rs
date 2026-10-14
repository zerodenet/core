use super::*;
use zero_proxy::ConfigApplyReconciler;

#[tokio::test]
async fn dispatcher_replacement_recovers_facts_after_storage_backpressure() {
    let dir = tempfile::tempdir().unwrap();
    let events = dir.path().join("events.jsonl");
    let mut config=RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[],"outbounds":[],"route":{"final":{"type":"direct"}},
        "api":{"event_sinks":[{"type":"jsonl","tag":"audit","path":events,"events":["engine.warning"]}],
            "outbox_path":dir.path().join("outbox.jsonl"),
            "dispatcher":{"max_in_memory_deliveries":1,"replay_batch_size":1,"outbox_min_free_bytes":u64::MAX,"outbox_min_free_percent":1}}
    }).to_string()).unwrap();
    let engine = Engine::new(config.clone()).unwrap();
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    for index in 0..24 {
        engine.emit_warning(&format!("recover-{index}"), "fact before reload");
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = services.state.lock().await;
            let status = state.dispatcher.as_ref().unwrap().sink_status();
            if status[0].last_error.is_some() {
                break;
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    config.api.dispatcher.outbox_min_free_bytes = 1;
    // Keep a valid percentage reserve, but do not let the production 5%
    // default mask recovery of this fixture's deliberately blocked PUTs.
    // A failed replacement must not consume the old continuation. The next
    // reconciliation uses it instead of adopting a fresh snapshot watermark.
    let mut failed = config.clone();
    let blocked_path = dir.path().join("directory-not-a-file");
    std::fs::create_dir(&blocked_path).unwrap();
    if let zero_config::EventSinkConfig::JsonLines { path, .. } = &mut failed.api.event_sinks[0] {
        *path = blocked_path.display().to_string();
    }
    assert!(services
        .reconcile(std::sync::Arc::new(failed))
        .await
        .is_err());

    tokio::time::timeout(
        Duration::from_secs(2),
        services.reconcile(std::sync::Arc::new(config)),
    )
    .await
    .unwrap()
    .unwrap();
    wait_for_file_text(&events, "recover-23").await;
    let content = std::fs::read_to_string(&events).unwrap();
    let rows = content
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 24);
    for index in 0..24 {
        assert_eq!(
            rows.iter()
                .filter(|row| row["payload"]["code"] == format!("recover-{index}"))
                .count(),
            1
        );
    }
    services.shutdown_status_monitor().await;
    services.shutdown_dispatcher().await;
}
