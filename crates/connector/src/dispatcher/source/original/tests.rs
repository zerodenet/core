use super::*;
fn engine() -> zero_engine::EngineHandle {
    zero_engine::EngineHandle::new(
        zero_engine::Engine::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap(),
    )
}
fn emit(engine: &zero_engine::EngineHandle, id: &str) {
    engine.emit(zero_api::ApiEvent::new(
        id,
        "kernel.original",
        1,
        serde_json::json!({"original": "雪".repeat(1024)}),
    ));
}
#[test]
fn actual_live_registration_uses_cursor_without_snapshot_and_transfers_same_payload_after_credit() {
    let engine = engine();
    let source = Source::kernel(engine.clone());
    let (live, baseline) = source.subscribe().unwrap();
    let baseline = baseline.unwrap();
    assert!(
        live.try_receive().is_none(),
        "no discarded initial flow DTO is allocated"
    );
    emit(&engine, "live-original");
    let record = live.receive(Duration::from_secs(3)).unwrap();
    let pointer = record.payload["original"].as_str().unwrap().as_ptr();
    {
        let history = engine.inner().original_event_history();
        let (_, _, original) = history.next(baseline);
        assert_eq!(
            original.unwrap().payload["original"]
                .as_str()
                .unwrap()
                .as_ptr(),
            pointer
        );
    }
    let budget = SourceBudget::new(1, 64);
    let credit = budget.try_reserve_event(&record).unwrap();
    let shared = record.into_shared();
    assert_eq!(
        shared.payload["original"].as_str().unwrap().as_ptr(),
        pointer
    );
    assert_eq!(budget.snapshot().reserved_bytes, 64);
    assert_eq!(budget.snapshot().oversized_events, 1);
    drop(credit);
    assert_eq!(budget.snapshot().reserved_events, 0);
}
#[test]
fn full_actual_source_credit_retains_history_and_replays_same_original_after_return() {
    let engine = engine();
    let source = Source::kernel(engine.clone());
    let sequence = engine.inner().latest_event_sequence();
    emit(&engine, "replay-original");
    let budget = SourceBudget::new(1, 64);
    let (pointer, count, occupied) = {
        let history = engine.inner().original_event_history();
        let (_, _, original) = history.next(sequence);
        let original = original.unwrap();
        (
            Arc::as_ptr(original),
            Arc::strong_count(original),
            budget.try_reserve_event(original).unwrap(),
        )
    };
    let (actual, gap, event) = source.next(sequence, &budget).unwrap();
    assert_eq!(actual, sequence + 1);
    assert!(!gap && event.is_none());
    {
        let history = engine.inner().original_event_history();
        assert_eq!(Arc::strong_count(history.next(sequence).2.unwrap()), count);
    }
    drop(occupied);
    let (_, _, event) = source.next(sequence, &budget).unwrap();
    let event = event.unwrap();
    assert_eq!(Arc::as_ptr(&event.event), pointer);
    assert_eq!(budget.snapshot().reserved_events, 1);
    drop(event);
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    let bootstrap = source.bootstrap(&budget).unwrap().unwrap();
    assert_eq!(bootstrap.event.event_type, event_type::ENGINE_STARTED);
    assert_eq!(budget.snapshot().reserved_events, 1);
    drop(bootstrap);
    assert_eq!(budget.snapshot().reserved_events, 0);
}
#[tokio::test]
async fn actual_kernel_bridge_backpressures_original_payload_and_joins_its_same_thread() {
    let engine = engine();
    let source = Source::kernel(engine.clone());
    let (live, _) = source.subscribe().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let budget = SourceBudget::new(1, 65_536);
    let (mut rx, book, task) = super::super::bridge::spawn(live, stop.clone(), 1, 65_536, budget);
    emit(&engine, "first");
    let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        book.snapshot().reserved_bytes * 2 < 65_536,
        "bytes can fit both originals; the actual event count must backpressure"
    );
    emit(&engine, "second");
    tokio::time::timeout(Duration::from_secs(3), async {
        while book.snapshot().waiting_event_bytes == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(book.snapshot().reserved_events, 1);
    assert!(rx.try_recv().is_err());
    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.event.event_id, "second");
    stop.store(true, Ordering::Release);
    rx.close();
    book.close();
    tokio::task::spawn_blocking(move || task.join().unwrap())
        .await
        .unwrap();
    assert_eq!(
        book.snapshot().reserved_events,
        1,
        "the delivered original is still held"
    );
    drop(second);
    assert_eq!(book.snapshot().reserved_events, 0);
}

#[cfg(feature = "sink-jsonl")]
async fn rows(path: &std::path::Path, expected: usize) -> Vec<RawApiEvent> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let text = std::fs::read_to_string(path).unwrap_or_default();
            let rows = text
                .lines()
                .filter_map(|line| serde_json::from_str::<RawApiEvent>(line).ok())
                .collect::<Vec<_>>();
            if rows.len() >= expected {
                return rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
#[cfg(feature = "sink-jsonl")]
#[tokio::test]
async fn ordinary_kernel_installation_preserves_bootstrap_live_and_replay_across_actual_retirement()
{
    let engine = engine();
    emit(&engine, "before-first-registration");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original-events.jsonl");
    let mut api = ApiConfig {
        event_sinks: vec![zero_config::EventSinkConfig::JsonLines {
            tag: "same-sink".into(),
            path: path.display().to_string(),
            events: Vec::new(),
            source_id: None,
        }],
        ..Default::default()
    };
    api.dispatcher.replay_batch_size = 1;
    api.dispatcher.source_queue_max_bytes = 64;
    let mut first = prepare_kernel_event_dispatcher_paused_with_network(
        engine.clone(),
        api.clone(),
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        true,
        None,
    );
    assert!(first.wait_ready().await.unwrap());
    first.activate().unwrap();
    emit(&engine, "first-live");
    let first_rows = rows(&path, 2).await;
    assert_eq!(first_rows.len(), 2);
    assert_eq!(first_rows[0].event_type, event_type::ENGINE_STARTED);
    assert_eq!(first_rows[1].event_id, "first-live");
    let continuation = first.shutdown_for_reload_retained().await.unwrap();
    emit(&engine, "between-original-registrations");
    let mut second = prepare_kernel_event_dispatcher_paused_with_network(
        engine.clone(),
        api,
        None,
        EventDispatcherOptions::default(),
        EventDispatcherNetwork::system(),
        false,
        Some(continuation),
    );
    assert!(second.wait_ready().await.unwrap());
    second.activate().unwrap();
    emit(&engine, "second-live");
    let all = rows(&path, 4).await;
    assert_eq!(
        all.iter()
            .map(|event| event.event_id.as_str())
            .skip(1)
            .collect::<Vec<_>>(),
        [
            "first-live",
            "between-original-registrations",
            "second-live"
        ]
    );
    assert_eq!(
        all.iter()
            .filter(|event| event.event_type == event_type::ENGINE_STARTED)
            .count(),
        1
    );
    assert!(all
        .iter()
        .all(|event| event.event_type != event_type::FLOW_SNAPSHOT));
    second.shutdown_retained().await.unwrap();
}

#[tokio::test]
async fn missing_replay_prefix_gets_same_n1_credit_before_pending_future_live_original() {
    let engine = engine();
    let source = Source::kernel(engine.clone());
    let (live, baseline) = source.subscribe().unwrap();
    let baseline = baseline.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let budget = SourceBudget::new(1, 64);
    let (mut rx, book, task) = super::super::bridge::spawn(live, stop.clone(), 1, 64, budget);
    emit(&engine, "missing-prefix");
    let first = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let original = Arc::as_ptr(&first.event);
    emit(&engine, "future-live");
    tokio::time::timeout(Duration::from_secs(3), async {
        while book.snapshot().waiting_event_bytes == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(source.next(baseline, &book).unwrap().2.is_none());
    assert!(book.snapshot().paused);
    drop(first);
    let replay = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(event) = source.next(baseline, &book).unwrap().2 {
                break event;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(Arc::as_ptr(&replay.event), original);
    assert_eq!(book.snapshot().reserved_events, 1);
    assert!(rx.try_recv().is_err());
    drop(replay);
    let future = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(future.event.event_id, "future-live");
    stop.store(true, Ordering::Release);
    rx.close();
    book.close();
    tokio::task::spawn_blocking(move || task.join().unwrap())
        .await
        .unwrap();
    drop(future);
    assert_eq!(book.snapshot().reserved_events, 0);
}
