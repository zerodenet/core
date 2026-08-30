#![cfg(feature = "sink-jsonl")]

use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use zero_api::{event_type, ApiEvent, EventFilter, EventSource, EventStream, RawApiEvent};
use zero_config::{ApiConfig, EventDispatcherConfig, EventSinkConfig};
use zero_connector::{
    spawn_event_dispatcher, spawn_event_dispatcher_with_engine_started, EventDispatcherOptions,
};

#[derive(Clone)]
struct StaticEventSource {
    events: Arc<Mutex<Vec<RawApiEvent>>>,
}

struct StaticEventStream {
    events: Mutex<VecDeque<RawApiEvent>>,
}

impl EventStream for StaticEventStream {
    fn recv(&self) -> Option<RawApiEvent> {
        self.try_recv()
    }

    fn try_recv(&self) -> Option<RawApiEvent> {
        self.events.lock().expect("events lock").pop_front()
    }
}

impl EventSource for StaticEventSource {
    type Stream = StaticEventStream;

    fn subscribe(&self, _filter: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(StaticEventStream {
            events: Mutex::new(self.events.lock().expect("events lock").clone().into()),
        })
    }

    fn latest(&self, limit: usize, _filter: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        Ok(self
            .events
            .lock()
            .expect("events lock")
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }

    fn since(
        &self,
        sequence: u64,
        limit: usize,
        _filter: EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        let events: Vec<_> = self
            .events
            .lock()
            .expect("events lock")
            .iter()
            .filter(|event| event.sequence.is_some_and(|value| value > sequence))
            .take(limit)
            .cloned()
            .collect();
        let actual_from = events
            .first()
            .and_then(|event| event.sequence)
            .unwrap_or_else(|| sequence.saturating_add(1));
        Ok(zero_api::EventReplay {
            core_instance_id: "test-core".to_owned(),
            requested_after: sequence,
            actual_from,
            has_gap: actual_from > sequence.saturating_add(1),
            events,
        })
    }
}

#[tokio::test]
async fn startup_bootstrap_deduplicates_event_already_in_live_subscription() {
    let path = temp_path("zero-connector-engine-started.jsonl");
    let _ = fs::remove_file(&path);
    let mut started = ApiEvent::new(
        "engine-instance:engine-1",
        event_type::ENGINE_STARTED,
        1_760_000_000_000,
        json!({ "build_id": "test" }),
    );
    started.sequence = Some(1);
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(vec![started])),
    };
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "startup-events".to_owned(),
            path: path.display().to_string(),
            events: vec![event_type::ENGINE_STARTED.to_owned()],
            source_id: None,
        }],
        ..Default::default()
    };

    let dispatcher = spawn_event_dispatcher_with_engine_started(
        source,
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn dispatcher")
    .expect("dispatcher handle");

    wait_for_file_contains(&path, "engine.started").await;
    dispatcher.shutdown().await;
    let written = fs::read_to_string(&path).expect("read startup events");
    let _ = fs::remove_file(&path);

    assert_eq!(written.lines().count(), 1, "startup event was duplicated");
}

#[tokio::test]
async fn dispatcher_writes_matching_events_to_jsonl_sink() {
    let path = temp_path("zero-connector-events.jsonl");
    let _ = fs::remove_file(&path);

    let mut event = ApiEvent::new(
        "event-1",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let mut snapshot = ApiEvent::new(
        "snapshot-0",
        event_type::FLOW_SNAPSHOT,
        1_760_000_000_000,
        json!({ "watermark": 0, "records": [] }),
    );
    snapshot.sequence = Some(0);
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(vec![snapshot, event])),
    };
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "local-events".to_owned(),
            path: path.display().to_string(),
            events: Vec::new(),
            source_id: Some("test-source".to_owned()),
        }],
        control: Default::default(),
        ..Default::default()
    };

    let dispatcher = spawn_event_dispatcher(
        source,
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn dispatcher")
    .expect("dispatcher handle");

    let status_handle = dispatcher.status_handle();
    let written = wait_for_file_contains(&path, "event-1").await;
    dispatcher.shutdown().await;
    let sink_status = status_handle.sink_status();
    let _ = fs::remove_file(&path);

    assert_eq!(sink_status.len(), 1);
    assert_eq!(sink_status[0].name, "local-events");
    assert!(sink_status[0].total_delivered >= 1);

    let line = written.lines().next().expect("jsonl line");
    let value = serde_json::from_str::<serde_json::Value>(line).expect("event json");
    assert_eq!(value["event_id"], "event-1");
    assert_eq!(value["source_id"], "test-source");
    assert!(!written.contains("snapshot-0"));
}

#[tokio::test]
async fn outbox_only_workset_still_delivers_fact_events() {
    let events_path = temp_path("zero-connector-outbox-only-events.jsonl");
    let outbox_path = temp_path("zero-connector-outbox-only.jsonl");
    cleanup_persistent_path(&events_path);
    cleanup_persistent_path(&outbox_path);

    let mut event = ApiEvent::new(
        "outbox-only-event",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "outbox-only".to_owned(),
            path: events_path.display().to_string(),
            events: Vec::new(),
            source_id: None,
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        dispatcher: EventDispatcherConfig {
            max_in_memory_deliveries: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![event])),
        },
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn outbox-only dispatcher")
    .expect("outbox-only dispatcher");

    let written = wait_for_file_contains(&events_path, "outbox-only-event").await;
    dispatcher.shutdown().await;

    assert!(written.contains("outbox-only-event"));
    cleanup_persistent_path(&events_path);
    cleanup_persistent_path(&outbox_path);
}

#[tokio::test]
async fn outbox_only_workset_makes_progress_for_every_sink() {
    let first_path = temp_path("zero-connector-outbox-only-first.jsonl");
    let second_path = temp_path("zero-connector-outbox-only-second.jsonl");
    let outbox_path = temp_path("zero-connector-outbox-only-multi.jsonl");
    for path in [&first_path, &second_path, &outbox_path] {
        cleanup_persistent_path(path);
    }

    let mut event = ApiEvent::new(
        "outbox-only-multi-event",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let sinks = [("first", &first_path), ("second", &second_path)]
        .into_iter()
        .map(|(tag, path)| EventSinkConfig::JsonLines {
            tag: tag.to_owned(),
            path: path.display().to_string(),
            events: Vec::new(),
            source_id: None,
        })
        .collect();
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![event])),
        },
        ApiConfig {
            event_sinks: sinks,
            outbox_path: Some(outbox_path.display().to_string()),
            dispatcher: EventDispatcherConfig {
                max_in_memory_deliveries: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn multi-sink outbox-only dispatcher")
    .expect("multi-sink outbox-only dispatcher");

    let first = wait_for_file_contains(&first_path, "outbox-only-multi-event").await;
    let second = wait_for_file_contains(&second_path, "outbox-only-multi-event").await;
    dispatcher.shutdown().await;

    assert!(first.contains("outbox-only-multi-event"));
    assert!(second.contains("outbox-only-multi-event"));
    for path in [&first_path, &second_path, &outbox_path] {
        cleanup_persistent_path(path);
    }
}

#[tokio::test]
async fn graceful_shutdown_flushes_ready_deliveries_without_an_outbox() {
    let path = temp_path("zero-connector-shutdown-flush.jsonl");
    let _ = fs::remove_file(&path);
    let events = (1..=100)
        .map(|sequence| {
            let mut event = ApiEvent::new(
                format!("shutdown-event-{sequence}"),
                event_type::FLOW_COMPLETED,
                1_760_000_000_000 + sequence,
                json!({ "sequence": sequence }),
            );
            event.sequence = Some(sequence);
            event
        })
        .collect::<Vec<_>>();
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        ApiConfig {
            event_sinks: vec![EventSinkConfig::JsonLines {
                tag: "shutdown-events".to_owned(),
                path: path.display().to_string(),
                events: Vec::new(),
                source_id: None,
            }],
            ..Default::default()
        },
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_secs(1),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn shutdown dispatcher")
    .expect("shutdown dispatcher handle");

    dispatcher.shutdown().await;

    assert_eq!(
        fs::read_to_string(&path)
            .expect("shutdown output")
            .lines()
            .count(),
        100
    );
    let _ = fs::remove_file(path);
}

#[tokio::test]
async fn dispatcher_pauses_before_outbox_consumes_the_filesystem_reserve() {
    let directory = tempfile::tempdir().expect("dispatcher reserve directory");
    let events_path = directory.path().join("events.jsonl");
    let outbox_path = directory.path().join("outbox.jsonl");
    let events = (1..=2)
        .map(|sequence| {
            let mut event = ApiEvent::new(
                format!("reserve-event-{sequence}"),
                event_type::FLOW_COMPLETED,
                1_760_000_000_000 + sequence,
                json!({ "sequence": sequence }),
            );
            event.sequence = Some(sequence);
            event
        })
        .collect();
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        ApiConfig {
            event_sinks: vec![EventSinkConfig::JsonLines {
                tag: "reserve-protected".to_owned(),
                path: events_path.display().to_string(),
                events: Vec::new(),
                source_id: None,
            }],
            outbox_path: Some(outbox_path.display().to_string()),
            dispatcher: EventDispatcherConfig {
                outbox_min_free_bytes: u64::MAX,
                outbox_min_free_percent: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn reserve-protected dispatcher")
    .expect("reserve-protected dispatcher handle");
    let status = dispatcher.status_handle();

    let sink = wait_for_outbox_write_block(&status).await;
    assert_eq!(sink.pending, 0);
    assert_eq!(sink.total_delivered, 0);
    assert!(
        sink.last_error
            .as_deref()
            .is_some_and(|error| error.contains("paused to preserve disk space")),
        "{:?}",
        sink.last_error
    );
    assert_eq!(
        fs::read_to_string(&events_path)
            .expect("reserved sink file")
            .lines()
            .count(),
        0
    );
    assert_eq!(
        fs::metadata(&outbox_path).expect("reserved outbox").len(),
        0
    );

    dispatcher.shutdown().await;
}

#[tokio::test]
async fn dispatcher_backs_off_then_recovers_after_outbox_capacity_returns() {
    const BALLAST_BYTES: usize = 32 * 1024 * 1024;
    const RETRY_DELAY: Duration = Duration::from_millis(500);

    let directory = tempfile::tempdir().expect("dispatcher retry directory");
    let events_path = directory.path().join("events.jsonl");
    let outbox_path = directory.path().join("outbox.jsonl");
    let ballast_path = directory.path().join("ballast.bin");
    let available_before =
        fs2::available_space(directory.path()).expect("available space before ballast");
    let mut ballast = vec![0_u8; BALLAST_BYTES];
    let mut state = 0x9e37_79b9_u32;
    for byte in &mut ballast {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        *byte = state as u8;
    }
    let mut ballast_file = fs::File::create(&ballast_path).expect("create ballast");
    ballast_file.write_all(&ballast).expect("write ballast");
    ballast_file.sync_all().expect("sync ballast");
    drop(ballast_file);
    drop(ballast);
    let available_with_ballast =
        fs2::available_space(directory.path()).expect("available space with ballast");
    let reclaimed_bytes = available_before.saturating_sub(available_with_ballast);
    assert!(
        reclaimed_bytes >= BALLAST_BYTES as u64 / 2,
        "test filesystem did not allocate enough ballast: {reclaimed_bytes} bytes"
    );
    let reserve_bytes = available_with_ballast.saturating_add(reclaimed_bytes / 2);

    let mut event = ApiEvent::new(
        "reserve-recovery-event",
        event_type::FLOW_COMPLETED,
        1_760_000_000_001,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![event])),
        },
        ApiConfig {
            event_sinks: vec![EventSinkConfig::JsonLines {
                tag: "reserve-protected".to_owned(),
                path: events_path.display().to_string(),
                events: Vec::new(),
                source_id: None,
            }],
            outbox_path: Some(outbox_path.display().to_string()),
            dispatcher: EventDispatcherConfig {
                retry_initial_delay_ms: RETRY_DELAY.as_millis() as u64,
                retry_max_delay_ms: RETRY_DELAY.as_millis() as u64,
                outbox_min_free_bytes: reserve_bytes,
                // This direct runtime test isolates the absolute reserve. The
                // validated production configuration still requires 1..=50.
                outbox_min_free_percent: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn reserve-recovery dispatcher")
    .expect("reserve-recovery dispatcher handle");
    let status = dispatcher.status_handle();

    wait_for_outbox_write_block(&status).await;
    fs::remove_file(&ballast_path).expect("release ballast capacity");
    wait_for_available_space(directory.path(), reserve_bytes).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        fs::read_to_string(&events_path)
            .map(|content| !content.contains("reserve-recovery-event"))
            .unwrap_or(true),
        "blocked event retried before its persistence deadline"
    );

    let written = wait_for_file_contains(&events_path, "reserve-recovery-event").await;
    assert!(written.contains("reserve-recovery-event"));
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn dispatcher_replays_a_live_queue_gap_and_keeps_a_monotonic_gap_counter() {
    let path = temp_path("zero-connector-replayed-events.jsonl");
    let _ = fs::remove_file(&path);
    let mut snapshot = ApiEvent::new(
        "snapshot-0",
        event_type::FLOW_SNAPSHOT,
        1_760_000_000_000,
        json!({ "watermark": 0, "records": [] }),
    );
    snapshot.sequence = Some(0);
    let mut event = ApiEvent::new(
        "event-after-gap",
        event_type::FLOW_COMPLETED,
        1_760_000_000_005,
        json!({ "value": 5 }),
    );
    event.sequence = Some(5);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "local-events".to_owned(),
            path: path.display().to_string(),
            events: Vec::new(),
            source_id: Some("gap-source".to_owned()),
        }],
        ..Default::default()
    };
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![snapshot, event])),
        },
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn gap dispatcher")
    .expect("gap dispatcher handle");
    let status = dispatcher.status_handle();

    let written = wait_for_file_contains(&path, "event-after-gap").await;
    let sink = status
        .sink_status()
        .into_iter()
        .find(|item| item.name == "local-events")
        .expect("gap sink status");
    dispatcher.shutdown().await;
    let _ = fs::remove_file(&path);

    assert!(written.contains("event-after-gap"));
    assert_eq!(sink.replay_gaps, 1);
    assert!(sink.total_failed >= 1);
}

#[tokio::test]
async fn jsonl_sink_rejects_a_second_live_owner_and_recovers_after_release() {
    let path = temp_path("zero-connector-exclusive-jsonl.jsonl");
    cleanup_persistent_path(&path);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "exclusive-jsonl".to_owned(),
            path: path.display().to_string(),
            events: Vec::new(),
            source_id: None,
        }],
        ..Default::default()
    };
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let options = EventDispatcherOptions {
        poll_interval: Duration::from_millis(10),
        max_retry_attempts: 1,
    };
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options)
        .expect("spawn first jsonl owner")
        .expect("first jsonl owner handle");

    let error = match spawn_event_dispatcher(source.clone(), api.clone(), None, options) {
        Err(error) => error,
        Ok(_) => panic!("second dispatcher must not share a jsonl sink"),
    };
    assert!(error.to_string().contains("already owned"));

    first.shutdown().await;
    let restarted = spawn_event_dispatcher(source, api, None, options)
        .expect("restart jsonl owner after lease release")
        .expect("restarted jsonl owner handle");
    restarted.shutdown().await;
    cleanup_persistent_path(&path);
}

#[tokio::test]
async fn dead_letter_sink_rejects_a_second_live_owner_and_recovers_after_release() {
    let path = temp_path("zero-connector-exclusive-dead-letter.jsonl");
    cleanup_persistent_path(&path);
    let api = ApiConfig {
        dead_letter_path: Some(path.display().to_string()),
        ..Default::default()
    };
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let options = EventDispatcherOptions {
        poll_interval: Duration::from_millis(10),
        max_retry_attempts: 1,
    };
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options)
        .expect("spawn first dead-letter owner")
        .expect("first dead-letter owner handle");

    let error = match spawn_event_dispatcher(source.clone(), api.clone(), None, options) {
        Err(error) => error,
        Ok(_) => panic!("second dispatcher must not share a dead-letter sink"),
    };
    assert!(error.to_string().contains("already owned"));

    first.shutdown().await;
    let restarted = spawn_event_dispatcher(source, api, None, options)
        .expect("restart dead-letter owner after lease release")
        .expect("restarted dead-letter owner handle");
    restarted.shutdown().await;
    cleanup_persistent_path(&path);
}

#[tokio::test]
async fn persistent_outbox_rejects_a_second_live_owner_and_recovers_after_release() {
    let events_path = temp_path("zero-connector-exclusive-events.jsonl");
    let outbox_path = temp_path("zero-connector-exclusive-outbox.jsonl");
    let _ = fs::remove_file(&events_path);
    let _ = fs::remove_file(&outbox_path);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "local-events".to_owned(),
            path: events_path.display().to_string(),
            events: Vec::new(),
            source_id: Some("exclusive-source".to_owned()),
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        ..Default::default()
    };
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let options = EventDispatcherOptions {
        poll_interval: Duration::from_millis(10),
        max_retry_attempts: 1,
    };
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options)
        .expect("spawn first exclusive dispatcher")
        .expect("first exclusive dispatcher handle");

    let error = match spawn_event_dispatcher(source.clone(), api.clone(), None, options) {
        Err(error) => error,
        Ok(_) => panic!("second dispatcher must not share a persistent outbox"),
    };
    assert!(error.to_string().contains("already owned"));

    first.shutdown().await;
    let restarted = spawn_event_dispatcher(source, api, None, options)
        .expect("restart dispatcher after releasing outbox lease")
        .expect("restarted exclusive dispatcher handle");
    restarted.shutdown().await;

    let _ = fs::remove_file(&events_path);
    let _ = fs::remove_file(&outbox_path);
    let mut lock_path = outbox_path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    let _ = fs::remove_file(std::path::PathBuf::from(lock_path));
}

#[tokio::test]
async fn dispatcher_compacts_a_large_outbox_and_restarts_without_redelivery() {
    let events_path = temp_path("zero-connector-compacted-events.jsonl");
    let outbox_path = temp_path("zero-connector-compacted-outbox.jsonl");
    let _ = fs::remove_file(&events_path);
    let _ = fs::remove_file(&outbox_path);
    let events = (1..=600)
        .map(|sequence| {
            let mut event = ApiEvent::new(
                format!("event-compact-{sequence}"),
                event_type::FLOW_COMPLETED,
                1_760_000_000_000 + sequence,
                json!({ "sequence": sequence }),
            );
            event.sequence = Some(sequence);
            event
        })
        .collect::<Vec<_>>();
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::JsonLines {
            tag: "local-events".to_owned(),
            path: events_path.display().to_string(),
            events: Vec::new(),
            source_id: Some("compact-source".to_owned()),
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        ..Default::default()
    };
    let first = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        api.clone(),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn compaction dispatcher")
    .expect("compaction dispatcher handle");
    wait_for_file_contains(&events_path, "event-compact-600").await;
    first.shutdown().await;

    let first_delivery = fs::read_to_string(&events_path).expect("compacted sink output");
    assert_eq!(first_delivery.lines().count(), 600);
    let compacted_journal = fs::read_to_string(&outbox_path).expect("compacted outbox");
    assert!(compacted_journal.lines().count() < 1_024);

    let restarted = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("restart compacted dispatcher")
    .expect("restarted compacted dispatcher handle");
    tokio::time::sleep(Duration::from_millis(50)).await;
    restarted.shutdown().await;
    let after_restart = fs::read_to_string(&events_path).expect("sink output after restart");
    assert_eq!(after_restart.lines().count(), 600);

    let _ = fs::remove_file(events_path);
    let _ = fs::remove_file(outbox_path);
}

async fn wait_for_file_contains(path: &std::path::Path, needle: &str) -> String {
    for _ in 0..1_000 {
        if let Ok(content) = fs::read_to_string(path) {
            if content.contains(needle) {
                return content;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("file did not contain `{needle}`");
}

async fn wait_for_outbox_write_block(
    status: &zero_connector::EventDispatcherStatusHandle,
) -> zero_api::SinkStatus {
    for _ in 0..1_000 {
        let sink = status
            .sink_status()
            .into_iter()
            .find(|sink| sink.name == "reserve-protected")
            .expect("reserve-protected status");
        if sink
            .outbox_storage
            .is_some_and(|storage| storage.write_blocked)
            && sink.last_error.is_some()
        {
            return sink;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("outbox storage did not enter the protected state");
}

async fn wait_for_available_space(path: &std::path::Path, required_bytes: u64) {
    for _ in 0..1_000 {
        if fs2::available_space(path).is_ok_and(|available| available > required_bytes) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("filesystem capacity did not recover above {required_bytes} bytes");
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!("{now}-{name}"))
}

fn cleanup_persistent_path(path: &std::path::Path) {
    let _ = fs::remove_file(path);
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    let _ = fs::remove_file(std::path::PathBuf::from(lock_path));
}
