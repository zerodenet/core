#![cfg(feature = "webhook")]

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::json;
use zero_api::{event_type, ApiEvent, EventFilter, EventSource, EventStream, RawApiEvent};
use zero_config::{ApiConfig, EventDispatcherConfig, EventSinkConfig};
use zero_connector::{spawn_event_dispatcher, EventDispatcherOptions};

#[derive(Clone)]
struct StaticEventSource {
    events: Arc<Mutex<Vec<RawApiEvent>>>,
}

struct StaticEventStream {
    events: Mutex<VecDeque<RawApiEvent>>,
}

#[derive(Clone)]
struct CountingEventSource {
    events: Arc<Mutex<Vec<RawApiEvent>>>,
    replay_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl EventSource for CountingEventSource {
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
        self.replay_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let events = self
            .events
            .lock()
            .expect("events lock")
            .iter()
            .filter(|event| event.sequence.is_some_and(|value| value > sequence))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        Ok(zero_api::EventReplay {
            core_instance_id: "test-core".to_owned(),
            requested_after: sequence,
            actual_from: events
                .first()
                .and_then(|event| event.sequence)
                .unwrap_or_else(|| sequence.saturating_add(1)),
            has_gap: false,
            events,
        })
    }
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
async fn dispatcher_posts_events_to_registered_webhook_with_opaque_headers() {
    let (url, server) = spawn_http_server();

    let mut event = ApiEvent::new(
        "event-1",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let source = StaticEventSource {
        events: Arc::new(Mutex::new(vec![event])),
    };
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url,
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("edge-test".to_owned()),
            headers: BTreeMap::from([(
                "authorization".to_owned(),
                "Bearer receiver-key".to_owned(),
            )]),
            allow_insecure: true,
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

    let request = server.join().expect("server thread");
    dispatcher.shutdown().await;

    assert!(request.starts_with("POST /events HTTP/1.1\r\n"));
    assert!(request
        .to_ascii_lowercase()
        .contains("authorization: bearer receiver-key"));
    let body = request_body(&request);
    let value = serde_json::from_str::<serde_json::Value>(body).expect("event json");
    assert_eq!(value["event_id"], "event-1");
    assert_eq!(value["source_id"], "edge-test");
}

#[tokio::test]
async fn dispatcher_routes_each_event_to_every_matching_webhook_registration() {
    let (traffic_url, traffic_server) = spawn_http_server();
    let (operations_url, operations_server) = spawn_http_server();

    let mut flow = ApiEvent::new(
        "event-flow",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "bytes": 42 }),
    );
    flow.sequence = Some(1);
    let mut warning = ApiEvent::new(
        "event-warning",
        event_type::ENGINE_WARNING,
        1_760_000_000_001,
        json!({ "message": "degraded" }),
    );
    warning.sequence = Some(2);

    let api = ApiConfig {
        event_sinks: vec![
            EventSinkConfig::Webhook {
                tag: "traffic-delivery".to_owned(),
                url: traffic_url,
                events: vec![event_type::FLOW_COMPLETED.to_owned()],
                source_id: None,
                headers: BTreeMap::new(),
                allow_insecure: true,
            },
            EventSinkConfig::Webhook {
                tag: "operations-delivery".to_owned(),
                url: operations_url,
                events: vec![event_type::ENGINE_WARNING.to_owned()],
                source_id: None,
                headers: BTreeMap::new(),
                allow_insecure: true,
            },
        ],
        ..Default::default()
    };

    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![flow, warning])),
        },
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn dispatcher")
    .expect("dispatcher handle");

    let traffic_request = traffic_server.join().expect("traffic server thread");
    let operations_request = operations_server.join().expect("operations server thread");
    dispatcher.shutdown().await;

    let traffic_event: serde_json::Value =
        serde_json::from_str(request_body(&traffic_request)).expect("traffic event json");
    let operations_event: serde_json::Value =
        serde_json::from_str(request_body(&operations_request)).expect("operations event json");
    assert_eq!(traffic_event["event_type"], event_type::FLOW_COMPLETED);
    assert_eq!(operations_event["event_type"], event_type::ENGINE_WARNING);
}

#[tokio::test]
async fn stalled_webhook_does_not_block_an_independent_registration() {
    let (stalled_url, stalled_received, release_stalled, stalled_server) =
        spawn_stalled_http_server();
    let (healthy_url, healthy_server) = spawn_http_server();

    let mut event = ApiEvent::new(
        "event-isolation",
        event_type::ENGINE_WARNING,
        1_760_000_000_000,
        json!({ "message": "isolate registrations" }),
    );
    event.sequence = Some(1);
    let api = ApiConfig {
        event_sinks: vec![
            EventSinkConfig::Webhook {
                tag: "stalled".to_owned(),
                url: stalled_url,
                events: vec![event_type::ENGINE_WARNING.to_owned()],
                source_id: None,
                headers: BTreeMap::new(),
                allow_insecure: true,
            },
            EventSinkConfig::Webhook {
                tag: "healthy".to_owned(),
                url: healthy_url,
                events: vec![event_type::ENGINE_WARNING.to_owned()],
                source_id: None,
                headers: BTreeMap::new(),
                allow_insecure: true,
            },
        ],
        dispatcher: EventDispatcherConfig {
            webhook_timeout_ms: 5_000,
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
    .expect("spawn isolated dispatcher")
    .expect("isolated dispatcher handle");

    stalled_received
        .recv_timeout(Duration::from_secs(2))
        .expect("stalled registration received request");
    let healthy_request = healthy_server
        .join()
        .expect("healthy registration must complete independently");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(request_body(&healthy_request))
            .expect("healthy event")["event_id"],
        "event-isolation"
    );

    release_stalled.send(()).expect("release stalled server");
    stalled_server.join().expect("stalled server");
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn stalled_webhook_does_not_spin_the_dispatcher() {
    let (url, received, release, server) = spawn_stalled_http_server();
    let replay_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = CountingEventSource {
        events: Arc::new(Mutex::new(vec![sequenced_event(
            "event-no-spin",
            event_type::ENGINE_WARNING,
            1,
        )])),
        replay_calls: replay_calls.clone(),
    };
    let dispatcher = spawn_event_dispatcher(
        source,
        webhook_api(url, None, 8),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(50),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn no-spin dispatcher")
    .expect("no-spin dispatcher");

    received
        .recv_timeout(Duration::from_secs(2))
        .expect("stalled request received");
    tokio::time::sleep(Duration::from_millis(350)).await;
    let calls = replay_calls.load(std::sync::atomic::Ordering::Relaxed);
    assert!(calls <= 12, "dispatcher replayed {calls} times while idle");

    release.send(()).expect("release stalled webhook");
    server.join().expect("stalled server");
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn durable_stalled_webhook_is_cancelled_on_shutdown_and_recovered() {
    let outbox_path = temp_path("zero-connector-cancelled-webhook-outbox.jsonl");
    let _ = std::fs::remove_file(&outbox_path);
    let (stalled_url, stalled_received, release_stalled, stalled_server) =
        spawn_stalled_http_server();
    let event = sequenced_event("event-cancelled-durable", event_type::FLOW_COMPLETED, 1);
    let api = webhook_api(stalled_url, Some(outbox_path.clone()), 8);
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![event])),
        },
        api.clone(),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn cancellable dispatcher")
    .expect("cancellable dispatcher");

    stalled_received
        .recv_timeout(Duration::from_secs(2))
        .expect("stalled webhook received durable delivery");
    tokio::time::timeout(Duration::from_secs(1), dispatcher.shutdown())
        .await
        .expect("durable webhook shutdown must cancel the request");
    release_stalled.send(()).expect("release cancelled server");
    stalled_server
        .join()
        .expect("stalled server exits on cancel");

    let (recovery_url, recovery_server) = spawn_http_server();
    let mut recovery_api = api;
    match &mut recovery_api.event_sinks[0] {
        EventSinkConfig::Webhook { url, .. } => *url = recovery_url,
        _ => unreachable!("webhook config"),
    }
    let recovered = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        recovery_api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn recovery dispatcher")
    .expect("recovery dispatcher");
    let request = recovery_server.join().expect("recovery request");
    assert!(request_body(&request).contains("event-cancelled-durable"));
    recovered.shutdown().await;
    let _ = std::fs::remove_file(outbox_path);
}

#[tokio::test]
async fn webhook_treats_non_retryable_http_status_as_final_rejection() {
    const BAD_REQUEST: &[u8] =
        b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let (url, server) = spawn_scripted_http_server(vec![BAD_REQUEST]);
    let mut event = ApiEvent::new(
        "event-rejected",
        event_type::ENGINE_WARNING,
        1_760_000_000_000,
        json!({"message": "rejected"}),
    );
    event.sequence = Some(1);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url,
            events: vec![event_type::ENGINE_WARNING.to_owned()],
            source_id: Some("edge-rejected".to_owned()),
            headers: BTreeMap::new(),
            allow_insecure: true,
        }],
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
            max_retry_attempts: 3,
        },
    )
    .expect("spawn dispatcher")
    .expect("dispatcher handle");

    let requests = server.join().expect("server thread");
    tokio::time::sleep(Duration::from_millis(30)).await;
    let status = dispatcher.sink_status();
    dispatcher.shutdown().await;

    assert_eq!(requests.len(), 1, "HTTP 400 must not be retried");
    let receiver = status
        .into_iter()
        .find(|item| item.name == "receiver")
        .expect("receiver status");
    assert_eq!(receiver.total_failed, 1);
    assert_eq!(receiver.pending, 0);
}

#[tokio::test]
async fn dispatcher_recovers_unacknowledged_webhook_delivery_from_outbox() {
    let outbox_path = temp_path("zero-connector-outbox.jsonl");
    let _ = std::fs::remove_file(&outbox_path);
    let (url, first_request_rx, server) = spawn_recovery_http_server();

    let mut event = ApiEvent::new(
        "event-recover-1",
        event_type::FLOW_COMPLETED,
        1_760_000_000_000,
        json!({ "value": 1 }),
    );
    event.sequence = Some(1);
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url,
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("edge-recovery".to_owned()),
            headers: BTreeMap::from([(
                "authorization".to_owned(),
                "Bearer receiver-key".to_owned(),
            )]),
            allow_insecure: true,
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        ..Default::default()
    };

    let first = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![event])),
        },
        api.clone(),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 3,
        },
    )
    .expect("spawn first dispatcher")
    .expect("first dispatcher handle");
    let first_status = first.status_handle();

    first_request_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("first failed delivery");
    wait_for_sink_pending(&first_status, "receiver", 1).await;
    wait_for_sink_failures(&first_status, "receiver", 1).await;
    let failed = first_status
        .sink_status()
        .into_iter()
        .find(|status| status.name == "receiver")
        .expect("receiver sink status after failure");
    assert_eq!(failed.pending, 1);
    assert_eq!(failed.total_failed, 1);
    first.shutdown().await;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&outbox_path)
        .expect("open outbox tail")
        .write_all(b"{\"op\":\"pu")
        .expect("write incomplete crash tail");
    let corrupt_generation =
        std::fs::read(&outbox_path).expect("read outbox generation before recovery");

    let mut recovery_api = api;
    // Recovery must still be able to write an ACK while new PUT records are
    // paused by the filesystem reserve.
    // Leave enough headroom above the sampled free space that unrelated file
    // deletion on a shared CI volume cannot make PUTs writable between this
    // probe and the dispatcher's storage-status probe.
    const PUT_BLOCK_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;
    recovery_api.dispatcher.outbox_min_free_bytes =
        fs2::available_space(outbox_path.parent().expect("outbox parent"))
            .expect("available outbox space")
            .saturating_add(PUT_BLOCK_HEADROOM_BYTES);
    recovery_api.dispatcher.outbox_min_free_percent = 1;
    let second = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        recovery_api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 3,
        },
    )
    .expect("spawn recovery dispatcher")
    .expect("recovery dispatcher handle");
    let second_status = second.status_handle();

    let requests = server.join().expect("recovery server thread");
    wait_for_sink_pending(&second_status, "receiver", 0).await;
    second.shutdown().await;

    let recovered = second_status
        .sink_status()
        .into_iter()
        .find(|status| status.name == "receiver")
        .expect("receiver sink status after recovery");
    assert_eq!(recovered.pending, 0);
    assert_eq!(recovered.total_delivered, 1);
    let recovery = recovered
        .outbox_recovery
        .as_ref()
        .expect("partial tail recovery status");
    assert_eq!(recovery.state, zero_api::OutboxRecoveryState::Recovered);
    assert_eq!(
        recovery.corruption_class,
        zero_api::OutboxCorruptionClass::PartialTail
    );
    assert_eq!(
        std::fs::read(&recovery.preserved_path).expect("read preserved partial tail"),
        corrupt_generation
    );
    assert!(
        recovered
            .outbox_storage
            .is_some_and(|storage| storage.write_blocked),
        "recovered ACK must drain even while new PUT records are blocked"
    );

    assert_eq!(requests.len(), 2);
    for request in requests {
        let body = request_body(&request);
        let value = serde_json::from_str::<serde_json::Value>(body).expect("event json");
        assert_eq!(value["event_id"], "event-recover-1");
        assert_eq!(value["source_id"], "edge-recovery");
    }
    let journal = std::fs::read_to_string(&outbox_path).expect("outbox journal");
    assert!(journal.contains("\"op\":\"put\""));
    assert!(journal.contains("\"op\":\"ack\""));
    let _ = std::fs::remove_file(outbox_path);
}

#[tokio::test]
async fn dispatcher_quarantines_corrupt_outbox_and_starts_degraded() {
    let directory = tempfile::tempdir().expect("corrupt outbox directory");
    let outbox_path = directory.path().join("outbox.jsonl");
    let corrupt = b"{\"op\":\"put\",\"delivery\":\n";
    std::fs::write(&outbox_path, corrupt).expect("write corrupt outbox record");
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url: "http://127.0.0.1:9/events".to_owned(),
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("edge-corrupt".to_owned()),
            headers: BTreeMap::from([(
                "authorization".to_owned(),
                "Bearer receiver-key".to_owned(),
            )]),
            allow_insecure: true,
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        ..Default::default()
    };

    let dispatcher = spawn_event_dispatcher(
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
    .expect("corrupt outbox must not prevent dispatcher startup")
    .expect("degraded dispatcher handle");
    let status = wait_for_outbox_recovery(&dispatcher.status_handle()).await;
    let recovery = status.outbox_recovery.expect("outbox recovery status");
    assert_eq!(
        recovery.state,
        zero_api::OutboxRecoveryState::RecoveryRequired
    );
    assert_eq!(
        recovery.corruption_class,
        zero_api::OutboxCorruptionClass::MalformedTail
    );
    assert!(recovery.message.contains("record 1 byte 0"));
    assert_eq!(
        std::fs::read(&recovery.preserved_path).expect("read preserved corrupt journal"),
        corrupt
    );
    assert_eq!(
        std::fs::read(&outbox_path).expect("read replacement journal"),
        b""
    );

    dispatcher.shutdown().await;
}

#[tokio::test]
async fn dispatcher_quarantines_malformed_middle_without_skipping_later_records() {
    let directory = tempfile::tempdir().expect("middle corruption directory");
    let outbox_path = directory.path().join("outbox.jsonl");
    let corrupt = b"{oops}\n{\"op\":\"ack\",\"sink_tag\":\"receiver\",\"event_id\":\"later\"}\n";
    std::fs::write(&outbox_path, corrupt).expect("write middle-corrupt outbox");
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        ApiConfig {
            event_sinks: vec![EventSinkConfig::Webhook {
                tag: "receiver".to_owned(),
                url: "http://127.0.0.1:9/events".to_owned(),
                events: vec![event_type::FLOW_COMPLETED.to_owned()],
                source_id: None,
                headers: BTreeMap::new(),
                allow_insecure: true,
            }],
            outbox_path: Some(outbox_path.display().to_string()),
            ..Default::default()
        },
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("middle corruption must not prevent dispatcher startup")
    .expect("degraded dispatcher handle");
    let status = wait_for_outbox_recovery(&dispatcher.status_handle()).await;
    let recovery = status.outbox_recovery.expect("outbox recovery status");
    assert_eq!(
        recovery.state,
        zero_api::OutboxRecoveryState::RecoveryRequired
    );
    assert_eq!(
        recovery.corruption_class,
        zero_api::OutboxCorruptionClass::MalformedMiddle
    );
    assert_eq!(
        std::fs::read(&recovery.preserved_path).expect("read quarantined generation"),
        corrupt
    );
    assert_eq!(
        std::fs::read(&outbox_path).expect("read new active generation"),
        b""
    );

    dispatcher.shutdown().await;
}

#[tokio::test]
async fn dispatcher_reopens_prior_v1_outbox_fixture_after_upgrade() {
    const SUCCESS: &[u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let directory = tempfile::tempdir().expect("legacy outbox directory");
    let outbox_path = directory.path().join("outbox.jsonl");
    std::fs::write(
        &outbox_path,
        include_bytes!("fixtures/outbox-v1-20260814.jsonl"),
    )
    .expect("copy legacy outbox fixture");
    let (url, server) = spawn_scripted_http_server(vec![SUCCESS]);
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        webhook_api(url, Some(outbox_path), 8),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 3,
        },
    )
    .expect("legacy v1 outbox must reopen")
    .expect("legacy fixture dispatcher");

    let requests = server.join().expect("legacy fixture server");
    wait_for_sink_pending(&dispatcher.status_handle(), "receiver", 0).await;
    dispatcher.shutdown().await;

    assert_eq!(requests.len(), 1);
    assert!(request_body(&requests[0]).contains("legacy-outbox-event"));
}

#[tokio::test]
async fn dispatcher_spills_backlog_to_disk_and_pages_a_bounded_working_set() {
    const SERVER_ERROR: &[u8] =
        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const SUCCESS: &[u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    let outbox_path = temp_path("zero-connector-bounded-outbox.jsonl");
    let _ = std::fs::remove_file(&outbox_path);
    let (url, server) = spawn_scripted_http_server(
        std::iter::repeat_n(SERVER_ERROR, 2)
            .chain(std::iter::repeat_n(SUCCESS, 5))
            .collect(),
    );
    let events = (1..=5)
        .map(|sequence| {
            let mut event = ApiEvent::new(
                format!("event-spill-{sequence}"),
                event_type::FLOW_COMPLETED,
                1_760_000_000_000 + sequence,
                json!({ "sequence": sequence }),
            );
            event.sequence = Some(sequence);
            event
        })
        .collect::<Vec<_>>();
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url,
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("edge-bounded".to_owned()),
            headers: BTreeMap::from([(
                "authorization".to_owned(),
                "Bearer receiver-key".to_owned(),
            )]),
            allow_insecure: true,
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        dispatcher: EventDispatcherConfig {
            max_in_memory_deliveries: 2,
            replay_batch_size: 16,
            ..Default::default()
        },
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
            max_retry_attempts: 10,
        },
    )
    .expect("spawn bounded dispatcher")
    .expect("bounded dispatcher handle");
    let first_status = first.status_handle();
    wait_for_sink_pending(&first_status, "receiver", 5).await;
    first.shutdown().await;

    let second = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        api,
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 10,
        },
    )
    .expect("spawn paged recovery dispatcher")
    .expect("paged recovery dispatcher handle");
    let second_status = second.status_handle();

    let requests = server.join().expect("scripted webhook server");
    wait_for_sink_pending(&second_status, "receiver", 0).await;
    second.shutdown().await;

    assert_eq!(requests.len(), 7);
    let mut duplicate_attempts = 0;
    for sequence in 1..=5 {
        let event_id = format!("event-spill-{sequence}");
        let deliveries = requests
            .iter()
            .filter(|request| request_body(request).contains(&event_id))
            .count();
        assert!(deliveries >= 1, "{event_id} was never delivered");
        duplicate_attempts += deliveries - 1;
    }
    assert_eq!(duplicate_attempts, 2);
    let recovered = second_status
        .sink_status()
        .into_iter()
        .find(|status| status.name == "receiver")
        .expect("paged receiver status");
    assert_eq!(recovered.pending, 0);
    assert_eq!(recovered.total_delivered, 5);
    let _ = std::fs::remove_file(outbox_path);
}

#[tokio::test]
async fn transient_samples_are_delivered_best_effort_but_never_persisted() {
    const SUCCESS: &[u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    let outbox_path = temp_path("zero-connector-sample-outbox.jsonl");
    let _ = std::fs::remove_file(&outbox_path);
    let (url, server) = spawn_scripted_http_server(vec![SUCCESS, SUCCESS]);
    let events = vec![
        sequenced_event("sample-not-durable", event_type::FLOW_UPDATED, 1),
        sequenced_event("fact-is-durable", event_type::FLOW_COMPLETED, 2),
    ];
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        webhook_api(url, Some(outbox_path.clone()), 8),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn best-effort dispatcher")
    .expect("best-effort dispatcher");

    let requests = server.join().expect("best-effort webhook server");
    dispatcher.shutdown().await;

    assert_eq!(requests.len(), 2);
    let journal = std::fs::read_to_string(&outbox_path).expect("sample outbox");
    assert!(journal.contains("fact-is-durable"));
    assert!(!journal.contains("sample-not-durable"));
    let _ = std::fs::remove_file(outbox_path);
}

#[tokio::test]
async fn full_memory_workset_replaces_samples_without_evicting_facts() {
    const SUCCESS: &[u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    let (url, server) = spawn_scripted_http_server(vec![SUCCESS, SUCCESS]);
    let events = vec![
        sequenced_event("sample-old", event_type::FLOW_UPDATED, 1),
        sequenced_event("sample-new", event_type::STATS_SAMPLED, 2),
        sequenced_event("fact-after-samples", event_type::FLOW_COMPLETED, 3),
    ];
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        webhook_api(url, None, 1),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn bounded sample dispatcher")
    .expect("bounded sample dispatcher");

    let requests = server.join().expect("bounded sample webhook server");
    dispatcher.shutdown().await;
    let bodies = requests
        .iter()
        .map(|request| request_body(request))
        .collect::<Vec<_>>();

    assert_eq!(bodies.len(), 2);
    assert!(bodies.iter().any(|body| body.contains("sample-new")));
    assert!(bodies
        .iter()
        .any(|body| body.contains("fact-after-samples")));
    assert!(!bodies.iter().any(|body| body.contains("sample-old")));
}

#[tokio::test]
async fn no_outbox_backpressure_preserves_every_fact_event() {
    const SUCCESS: &[u8] =
        b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    let (url, server) = spawn_scripted_http_server(vec![SUCCESS, SUCCESS, SUCCESS]);
    let events = (1..=3)
        .map(|sequence| {
            sequenced_event(
                &format!("bounded-fact-{sequence}"),
                event_type::FLOW_COMPLETED,
                sequence,
            )
        })
        .collect();
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        webhook_api(url, None, 1),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .expect("spawn bounded fact dispatcher")
    .expect("bounded fact dispatcher");

    let requests = server.join().expect("bounded fact webhook server");
    dispatcher.shutdown().await;
    let bodies = requests
        .iter()
        .map(|request| request_body(request))
        .collect::<Vec<_>>();

    assert_eq!(bodies.len(), 3);
    for sequence in 1..=3 {
        assert!(
            bodies
                .iter()
                .any(|body| body.contains(&format!("bounded-fact-{sequence}"))),
            "fact {sequence} was lost while applying backpressure"
        );
    }
}

fn sequenced_event(event_id: &str, event_type: &str, sequence: u64) -> RawApiEvent {
    let mut event = ApiEvent::new(
        event_id,
        event_type,
        1_760_000_000_000 + sequence,
        json!({ "sequence": sequence }),
    );
    event.sequence = Some(sequence);
    event
}

fn webhook_api(
    url: String,
    outbox_path: Option<std::path::PathBuf>,
    max_in_memory_deliveries: usize,
) -> ApiConfig {
    ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "receiver".to_owned(),
            url,
            events: Vec::new(),
            source_id: Some("bounded-test".to_owned()),
            headers: BTreeMap::new(),
            allow_insecure: true,
        }],
        outbox_path: outbox_path.map(|path| path.display().to_string()),
        dispatcher: EventDispatcherConfig {
            max_in_memory_deliveries,
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn wait_for_sink_pending(
    status: &zero_connector::EventDispatcherStatusHandle,
    sink: &str,
    expected: u64,
) {
    for _ in 0..100 {
        let pending = status
            .sink_status()
            .into_iter()
            .find(|item| item.name == sink)
            .map(|item| item.pending);
        if pending == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("sink `{sink}` pending count did not become {expected}");
}

async fn wait_for_sink_failures(
    status: &zero_connector::EventDispatcherStatusHandle,
    sink: &str,
    expected: u64,
) {
    for _ in 0..100 {
        let failures = status
            .sink_status()
            .into_iter()
            .find(|item| item.name == sink)
            .map(|item| item.total_failed);
        if failures == Some(expected) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("sink `{sink}` failure count did not become {expected}");
}

async fn wait_for_outbox_recovery(
    status: &zero_connector::EventDispatcherStatusHandle,
) -> zero_api::SinkStatus {
    for _ in 0..100 {
        if let Some(sink) = status
            .sink_status()
            .into_iter()
            .find(|sink| sink.outbox_recovery.is_some())
        {
            return sink;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("outbox recovery status did not become visible");
}

fn spawn_http_server() -> (String, JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local server");
    listener
        .set_nonblocking(true)
        .expect("set nonblocking listener");
    let address = listener.local_addr().expect("local address");

    let handle = thread::spawn(move || {
        for _ in 0..100 {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_nonblocking(false)
                        .expect("set blocking connection");
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .expect("read timeout");
                    let Some(request) = read_http_request(&mut stream) else {
                        continue;
                    };
                    stream
                        .write_all(
                            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .expect("write response");
                    return request;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) => panic!("accept request: {error}"),
            }
        }

        panic!("webhook request was not received");
    });

    (format!("http://{address}/events"), handle)
}

fn spawn_stalled_http_server() -> (String, mpsc::Receiver<()>, mpsc::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stalled server");
    let address = listener.local_addr().expect("stalled server address");
    let (received_tx, received_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let (_request, mut stream) = accept_http_request(&listener);
        received_tx.send(()).expect("signal stalled request");
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("release stalled response");
        let _ = stream.write_all(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
    });
    (
        format!("http://{address}/events"),
        received_rx,
        release_tx,
        handle,
    )
}

fn spawn_recovery_http_server() -> (String, mpsc::Receiver<()>, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind recovery server");
    listener
        .set_nonblocking(true)
        .expect("set recovery listener nonblocking");
    let address = listener.local_addr().expect("recovery server address");
    let (first_request_tx, first_request_rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for response in [
            None,
            Some(
                b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .as_slice(),
            ),
        ] {
            let mut request = accept_http_request(&listener);
            requests.push(request.0);
            if let Some(response) = response {
                request.1.write_all(response).expect("write response");
            }
            if requests.len() == 1 {
                first_request_tx.send(()).expect("signal first request");
            }
        }
        requests
    });

    (format!("http://{address}/events"), first_request_rx, handle)
}

fn spawn_scripted_http_server(responses: Vec<&'static [u8]>) -> (String, JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind scripted server");
    listener
        .set_nonblocking(true)
        .expect("set scripted listener nonblocking");
    let address = listener.local_addr().expect("scripted server address");
    let handle = thread::spawn(move || {
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let (request, mut stream) = accept_http_request(&listener);
            requests.push(request);
            stream.write_all(response).expect("write scripted response");
        }
        requests
    });
    (format!("http://{address}/events"), handle)
}

fn accept_http_request(listener: &TcpListener) -> (String, TcpStream) {
    for _ in 0..250 {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_nonblocking(false)
                    .expect("set blocking connection");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout");
                if let Some(request) = read_http_request(&mut stream) {
                    return (request, stream);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("accept request: {error}"),
        }
    }
    panic!("webhook request was not received");
}

fn read_http_request(stream: &mut TcpStream) -> Option<String> {
    let mut received = Vec::new();
    let mut buffer = [0_u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        received.extend_from_slice(&buffer[..read]);
        if let Some(position) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };

    let headers = String::from_utf8_lossy(&received[..header_end]);
    let content_length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())?;

    while received.len() < header_end + content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        received.extend_from_slice(&buffer[..read]);
    }

    String::from_utf8(received).ok()
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .expect("request body")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!("{unique}-{name}"))
}
