use super::*;

#[test]
fn blocked_persistence_does_not_drain_the_bounded_source_bridge() {
    child::run(
        "blocked_persistence_does_not_drain_the_bounded_source_bridge",
        blocked(8 * 1024 * 1024, 6, 4096, false),
    );
}

#[test]
fn blocked_persistence_respects_source_bytes_and_cancels_budget_waiters() {
    child::run(
        "blocked_persistence_respects_source_bytes_and_cancels_budget_waiters",
        blocked(2048, 2, 4096, false),
    );
}

#[test]
fn blocked_persistence_recovers_into_the_same_durable_journal() {
    child::run(
        "blocked_persistence_recovers_into_the_same_durable_journal",
        blocked(256, 2, 16, true),
    );
}

async fn blocked(bytes: u32, max_received: usize, event_count: u64, keep_outbox: bool) {
    let source = LiveSource::new("pressure-core");
    let dir = tempfile::tempdir().unwrap();
    let outbox = dir.path().join("outbox.jsonl");
    let delivered = dir.path().join("delivered.jsonl");
    let mut api = zero_config::ApiConfig {
        outbox_path: Some(outbox.display().to_string()),
        event_sinks: vec![zero_config::EventSinkConfig::JsonLines {
            tag: "receiver".into(),
            path: delivered.display().to_string(),
            events: vec![],
            source_id: None,
        }],
        ..Default::default()
    };
    api.dispatcher.max_in_memory_deliveries = 4;
    api.dispatcher.replay_batch_size = 4;
    api.dispatcher.source_queue_max_bytes = bytes;
    api.dispatcher.outbox_min_free_bytes = u64::MAX;
    api.dispatcher.retry_initial_delay_ms = 200;
    api.dispatcher.retry_max_delay_ms = 200;
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options())
        .unwrap()
        .unwrap();
    let mut initial = sequenced_event("pressure-1", event_type::FLOW_COMPLETED, 1);
    initial.payload = json!({"padding":"x".repeat(1024)});
    source.emit(initial);
    wait_for_sink_status(&first.status_handle(), "receiver", |status| {
        status.outbox_storage.is_some_and(|s| s.write_blocked)
    })
    .await;
    for sequence in 2..=event_count {
        let mut event = sequenced_event(
            &format!("pressure-{sequence}"),
            event_type::FLOW_COMPLETED,
            sequence,
        );
        event.payload = json!({"padding":"x".repeat(1024)});
        source.emit(event);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        source.received() <= max_received,
        "bounded admission drained {} source events",
        source.received()
    );
    let buffer = first.sink_status()[0].source_buffer.unwrap();
    assert!(buffer.paused);
    assert_eq!(buffer.byte_budget, u64::from(bytes));
    assert!(buffer.high_water_bytes > 0 && buffer.high_water_bytes <= u64::from(bytes));
    assert!(buffer.high_water_events <= max_received as u64);
    if bytes == 256 {
        assert!(buffer.oversized_events > 0);
    }
    // Budget acquisition and channel sends must both be cancellable at reload.
    let continuation = tokio::time::timeout(Duration::from_secs(1), first.shutdown_for_reload())
        .await
        .unwrap()
        .unwrap();
    api.dispatcher.outbox_min_free_bytes = 1;
    api.dispatcher.outbox_min_free_percent = 0;
    if !keep_outbox {
        // Nothing was admitted durably while writes were blocked. Bulk replay
        // exercises bounded recovery without thousands of unrelated fsyncs;
        // the separate same-journal case verifies durable PUT/ACK recovery.
        assert_eq!(std::fs::metadata(&outbox).unwrap().len(), 0);
        api.outbox_path = None;
    }
    let second = resume_event_dispatcher_with_network(
        source,
        api,
        None,
        options(),
        EventDispatcherNetwork::system(),
        continuation,
    )
    .unwrap()
    .unwrap();
    // Keep the bulk deadline local; do not loosen small delivery fixtures.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while second.sink_status()[0].total_delivered != event_count {
        assert!(
            std::time::Instant::now() < deadline,
            "recovery did not deliver every retained fact: {:?}",
            second.sink_status()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let second_status = second.status_handle();
    second.shutdown().await;
    let final_buffer = second_status.sink_status()[0].source_buffer.unwrap();
    assert_eq!(final_buffer.reserved_events, 0);
    assert_eq!(final_buffer.reserved_bytes, 0);
    assert_eq!(final_buffer.waiting_event_bytes, 0);
    let records = std::fs::read_to_string(delivered).unwrap();
    let ids = records
        .lines()
        .map(|line| serde_json::from_str::<RawApiEvent>(line).unwrap().event_id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), event_count as usize);
    for sequence in 1..=event_count {
        assert!(ids.contains(&format!("pressure-{sequence}")));
    }
}

#[test]
fn legacy_persisted_samples_are_delivered_and_acknowledged_after_upgrade() {
    child::run(
        "legacy_persisted_samples_are_delivered_and_acknowledged_after_upgrade",
        legacy_samples(),
    );
}

async fn legacy_samples() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let kinds = [
        event_type::FLOW_UPDATED,
        event_type::STATS_SAMPLED,
        event_type::STATS_SCOPES_SAMPLED,
        event_type::STATS_HOST_INTERFACES_SAMPLED,
        event_type::ENDPOINT_STATS_SAMPLED,
        event_type::STATS_RESET,
    ];
    let mut frames = String::new();
    for kind in kinds {
        frames.push_str(&serde_json::to_string(&json!({"op":"put", "delivery":{
            "sink_tag":"receiver", "event":sequenced_event(kind, kind, 1), "attempts":0, "message":null
        }})).unwrap());
        frames.push('\n');
    }
    std::fs::write(&path, frames).unwrap();
    let (url, server) = spawn_scripted_http_server(vec![
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            kinds.len()
        ]);
    let dispatcher = spawn_event_dispatcher(
        LiveSource::new("legacy-core"),
        fixture_api(url, Some(path.clone()), 2),
        None,
        options(),
    )
    .unwrap()
    .unwrap();
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
        s.total_delivered == kinds.len() as u64 && s.delivery.durable_pending == 0
    })
    .await;
    dispatcher.shutdown().await;
    let bodies = server.join().unwrap();
    for kind in kinds {
        assert!(bodies.iter().any(|body| body.contains(kind)));
    }
    let records = std::fs::read_to_string(path).unwrap();
    assert_eq!(
        records
            .lines()
            .filter(|line| line.contains("\"op\":\"ack\""))
            .count(),
        kinds.len()
    );
}
