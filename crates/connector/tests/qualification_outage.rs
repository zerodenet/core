#![cfg(feature = "webhook")]

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use zero_api::{event_type, ApiEvent, EventFilter, EventSource, EventStream, RawApiEvent};
use zero_config::{ApiConfig, EventDispatcherConfig, EventSinkConfig};
use zero_connector::{spawn_event_dispatcher, EventDispatcherOptions, EventDispatcherStatusHandle};

#[derive(Clone)]
struct OutageEventSource {
    events: Arc<Mutex<Vec<RawApiEvent>>>,
}

struct OutageEventStream {
    events: Mutex<VecDeque<RawApiEvent>>,
}

impl EventStream for OutageEventStream {
    fn recv(&self) -> Option<RawApiEvent> {
        self.try_recv()
    }

    fn try_recv(&self) -> Option<RawApiEvent> {
        self.events.lock().expect("events lock").pop_front()
    }
}

impl EventSource for OutageEventSource {
    type Stream = OutageEventStream;

    fn subscribe(&self, _filter: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(OutageEventStream {
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
        let events = self
            .events
            .lock()
            .expect("events lock")
            .iter()
            .filter(|event| event.sequence.is_some_and(|value| value > sequence))
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
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
#[ignore = "long-running Zero connector outage qualification"]
async fn connector_backlog_recovers_after_receiver_outage_without_losing_events() {
    let event_count = env_usize("ZERO_CONNECTOR_OUTAGE_EVENTS", 10_000);
    let timeout_seconds = env_u64("ZERO_CONNECTOR_SOAK_TIMEOUT_SECONDS", 900);
    let directory = tempfile::tempdir().expect("qualification directory");
    let outbox_path = directory.path().join("outbox.jsonl");
    let dead_letter_path = directory.path().join("dead-letter.jsonl");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind qualification receiver");
    let address = listener.local_addr().expect("webhook address");
    let receiver_online = Arc::new(AtomicBool::new(false));
    let failed_requests = Arc::new(AtomicUsize::new(0));
    let mut receiver = tokio::spawn(serve_with_outage(
        listener,
        event_count,
        receiver_online.clone(),
        failed_requests.clone(),
    ));
    let api = ApiConfig {
        event_sinks: vec![EventSinkConfig::Webhook {
            tag: "qualification-receiver".to_owned(),
            url: format!("http://{address}/events"),
            events: vec![event_type::FLOW_COMPLETED.to_owned()],
            source_id: Some("zero-outage-qualification".to_owned()),
            headers: BTreeMap::from([(
                "authorization".to_owned(),
                "Bearer qualification-key".to_owned(),
            )]),
            allow_insecure: true,
        }],
        outbox_path: Some(outbox_path.display().to_string()),
        dead_letter_path: Some(dead_letter_path.display().to_string()),
        dispatcher: EventDispatcherConfig {
            max_in_memory_deliveries: 64,
            replay_batch_size: 256,
            ..Default::default()
        },
        ..Default::default()
    };
    let options = EventDispatcherOptions {
        poll_interval: Duration::from_millis(1),
        max_retry_attempts: 100,
    };
    let events = build_events(event_count);
    let started = Instant::now();
    let dispatcher = spawn_event_dispatcher(
        OutageEventSource {
            events: Arc::new(Mutex::new(events)),
        },
        api.clone(),
        None,
        options,
    )
    .expect("spawn outage dispatcher")
    .expect("outage dispatcher handle");
    let status = dispatcher.status_handle();
    if !wait_for_offline_backlog(&status, Duration::from_secs(timeout_seconds)).await {
        receiver.abort();
        dispatcher.shutdown().await;
        panic!("offline backlog did not become durable");
    }

    receiver_online.store(true, Ordering::SeqCst);
    if !wait_for_pending(&status, 0, Duration::from_secs(timeout_seconds)).await {
        receiver.abort();
        dispatcher.shutdown().await;
        panic!(
            "outage backlog did not converge: {:?}",
            status.sink_status()
        );
    }
    let received =
        match tokio::time::timeout(Duration::from_secs(timeout_seconds), &mut receiver).await {
            Ok(Ok(received)) => received,
            Ok(Err(error)) => {
                dispatcher.shutdown().await;
                panic!("qualification receiver task failed: {error}");
            }
            Err(_) => {
                receiver.abort();
                dispatcher.shutdown().await;
                panic!("qualification receiver timed out");
            }
        };
    dispatcher.shutdown().await;

    assert_eq!(received.len(), event_count);
    assert!(failed_requests.load(Ordering::SeqCst) > 0);
    assert!(
        !dead_letter_path.exists()
            || std::fs::metadata(&dead_letter_path)
                .expect("dead letter metadata")
                .len()
                == 0,
        "recoverable receiver outage produced dead letters"
    );
    let completed = status
        .sink_status()
        .into_iter()
        .find(|item| item.name == "qualification-receiver")
        .expect("qualification receiver status");
    assert_eq!(completed.pending, 0);
    assert!(completed.total_failed > 0);
    assert_eq!(completed.total_delivered, event_count as u64);

    let restarted = spawn_event_dispatcher(
        OutageEventSource {
            events: Arc::new(Mutex::new(Vec::new())),
        },
        api,
        None,
        options,
    )
    .expect("restart outage dispatcher")
    .expect("restarted outage dispatcher handle");
    tokio::time::sleep(Duration::from_millis(250)).await;
    let restart_status = restarted
        .status_handle()
        .sink_status()
        .into_iter()
        .find(|item| item.name == "qualification-receiver")
        .expect("restarted qualification status");
    restarted.shutdown().await;
    assert_eq!(restart_status.pending, 0);
    assert_eq!(restart_status.total_delivered, 0);

    eprintln!(
        "zero connector outage qualification: events={event_count} elapsed_seconds={:.3} outbox_bytes={} failed_attempts={}",
        started.elapsed().as_secs_f64(),
        std::fs::metadata(&outbox_path)
            .expect("outbox metadata")
            .len(),
        completed.total_failed
    );
}

async fn serve_with_outage(
    listener: TcpListener,
    expected: usize,
    online: Arc<AtomicBool>,
    failed_requests: Arc<AtomicUsize>,
) -> HashSet<String> {
    let mut event_ids = HashSet::with_capacity(expected);
    while event_ids.len() < expected {
        let (mut socket, _) = listener.accept().await.expect("accept webhook");
        let request = read_request(&mut socket).await;
        if online.load(Ordering::SeqCst) {
            let body = request
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .expect("webhook body");
            let value: serde_json::Value = serde_json::from_str(body).expect("webhook JSON");
            event_ids.insert(value["event_id"].as_str().expect("event_id").to_owned());
            socket
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write webhook response");
        } else {
            failed_requests.fetch_add(1, Ordering::SeqCst);
            socket
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write outage response");
        }
    }
    event_ids
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let read = socket.read(&mut buffer).await.expect("read webhook");
        assert!(read > 0, "webhook closed before request completed");
        raw.extend_from_slice(&buffer[..read]);
        let Some(headers_end) = raw.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers_end = headers_end + 4;
        let headers = String::from_utf8_lossy(&raw[..headers_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length: ")
                    .map(str::to_owned)
            })
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if raw.len() >= headers_end + content_length {
            return String::from_utf8(raw).expect("UTF-8 webhook");
        }
    }
}

async fn wait_for_offline_backlog(status: &EventDispatcherStatusHandle, timeout: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            let sink = sink_status(status);
            if sink.pending > 0 && sink.total_failed > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

async fn wait_for_pending(
    status: &EventDispatcherStatusHandle,
    expected: u64,
    timeout: Duration,
) -> bool {
    tokio::time::timeout(timeout, async {
        loop {
            if sink_status(status).pending == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

fn sink_status(status: &EventDispatcherStatusHandle) -> zero_api::SinkStatus {
    status
        .sink_status()
        .into_iter()
        .find(|item| item.name == "qualification-receiver")
        .expect("qualification receiver status")
}

fn build_events(count: usize) -> Vec<RawApiEvent> {
    (1..=count)
        .map(|sequence| {
            let mut event = ApiEvent::new(
                format!("outage-flow-{sequence}"),
                event_type::FLOW_COMPLETED,
                1_760_000_000_000 + sequence as u64,
                json!({ "traffic": { "bytes_up": 1024, "bytes_down": 4096 } }),
            );
            event.sequence = Some(sequence as u64);
            event.principal_key = Some(format!("principal:{}", sequence % 1_000));
            event
        })
        .collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
