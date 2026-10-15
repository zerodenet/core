use super::*;
use zero_api::EventStreamReceive;
use zero_connector::resume_event_dispatcher_with_network;

#[path = "reload/source.rs"]
mod source;
use source::LiveSource;
#[path = "reload/backpressure.rs"]
mod backpressure;
#[path = "reload/child.rs"]
mod child;
#[path = "reload/delivery_bytes.rs"]
mod delivery_bytes;

fn options() -> EventDispatcherOptions {
    EventDispatcherOptions {
        poll_interval: Duration::from_millis(10),
        max_retry_attempts: 1,
    }
}

#[test]
fn reload_handoff_recovers_volatile_facts_beyond_the_bounded_bridge() {
    child::run(
        "reload_handoff_recovers_volatile_facts_beyond_the_bounded_bridge",
        run_reload_handoff_recovers_volatile_facts_beyond_the_bounded_bridge(),
    );
}

async fn run_reload_handoff_recovers_volatile_facts_beyond_the_bounded_bridge() {
    let source = LiveSource::new("handoff-core");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/events", listener.local_addr().unwrap());
    let (received_tx, received) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (known, mut stream) = accept_http_request(&listener);
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        let (_, mut stream) = accept_http_request(&listener);
        received_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let _ = stream.write_all(
            b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        known
    });
    let mut api = fixture_api(url, None, 1);
    api.dispatcher.replay_batch_size = 1;
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options())
        .unwrap()
        .unwrap();
    source.emit(sequenced_event("handoff-1", event_type::FLOW_COMPLETED, 1));
    wait_for_sink_status(&first.status_handle(), "receiver", |sink| {
        sink.total_delivered == 1
    })
    .await;
    for sequence in 2..=24 {
        source.emit(sequenced_event(
            &format!("handoff-{sequence}"),
            event_type::FLOW_COMPLETED,
            sequence,
        ));
    }
    received.recv_timeout(Duration::from_secs(2)).unwrap();
    let continuation = tokio::time::timeout(Duration::from_secs(1), first.shutdown_for_reload())
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    assert!(server.join().unwrap().contains("handoff-1"));
    let (url, server) = spawn_scripted_http_server(
        vec![b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"; 23],
    );
    match &mut api.event_sinks[0] {
        EventSinkConfig::Webhook { url: value, .. } => *value = url,
        _ => unreachable!(),
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
    let bodies = server.join().unwrap();
    second.shutdown().await;
    let ids = bodies
        .iter()
        .map(|body| {
            serde_json::from_str::<RawApiEvent>(request_body(body))
                .unwrap()
                .event_id
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), 23);
    assert!(
        !ids.contains("handoff-1"),
        "already acknowledged facts must not be replayed"
    );
    for sequence in 2..=24 {
        assert!(ids.contains(&format!("handoff-{sequence}")));
    }
}

#[test]
fn graceful_shutdown_drains_volatile_facts_without_an_outbox() {
    child::run(
        "graceful_shutdown_drains_volatile_facts_without_an_outbox",
        run_graceful_shutdown_drains_volatile_facts_without_an_outbox(),
    );
}

async fn run_graceful_shutdown_drains_volatile_facts_without_an_outbox() {
    let source = LiveSource::new("drain-core");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/events", listener.local_addr().unwrap());
    let (received_tx, received_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut ids = std::collections::HashSet::new();
        for index in 0..24 {
            let (request, mut stream) = accept_http_request(&listener);
            if index == 0 {
                received_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            ids.insert(
                serde_json::from_str::<RawApiEvent>(request_body(&request))
                    .unwrap()
                    .event_id,
            );
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
        ids
    });
    let mut api = fixture_api(url, None, 1);
    api.dispatcher.replay_batch_size = 1;
    let dispatcher = spawn_event_dispatcher(source.clone(), api, None, options())
        .unwrap()
        .unwrap();
    for sequence in 1..=24 {
        source.emit(sequenced_event(
            &format!("drain-{sequence}"),
            event_type::FLOW_COMPLETED,
            sequence,
        ));
    }
    received_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        release_tx.send(()).unwrap();
    });
    tokio::time::timeout(Duration::from_secs(4), dispatcher.shutdown())
        .await
        .unwrap();
    release.await.unwrap();
    assert_eq!(server.join().unwrap().len(), 24);
}

#[test]
fn all_statistics_samples_stay_out_of_durable_backlog_while_facts_survive() {
    child::run(
        "all_statistics_samples_stay_out_of_durable_backlog_while_facts_survive",
        run_all_statistics_samples_stay_out_of_durable_backlog_while_facts_survive(),
    );
}

async fn run_all_statistics_samples_stay_out_of_durable_backlog_while_facts_survive() {
    let source = LiveSource::new("sample-core");
    let (url, received, release, server) = spawn_stalled_http_server();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let mut api = fixture_api(url, Some(path.clone()), 2);
    api.dispatcher.replay_batch_size = 1;
    let dispatcher = spawn_event_dispatcher(source.clone(), api, None, options())
        .unwrap()
        .unwrap();
    source.emit(sequenced_event(
        "in-flight-fact",
        event_type::FLOW_COMPLETED,
        1,
    ));
    received.recv_timeout(Duration::from_secs(2)).unwrap();
    let kinds = [
        event_type::FLOW_UPDATED,
        event_type::STATS_SAMPLED,
        "stats.scopes_sampled",
        "stats.host_interfaces_sampled",
        "endpoint.stats_sampled",
        "stats.reset",
        "endpoint.state_changed",
    ];
    for (index, kind) in kinds.into_iter().enumerate() {
        source.emit(sequenced_event(kind, kind, index as u64 + 2));
    }
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |status| {
        status.delivery.durable_pending == 3
    })
    .await;
    let records = std::fs::read_to_string(&path).unwrap();
    for kind in &kinds[..5] {
        assert!(
            !records.contains(&format!("\"event_type\":\"{kind}\"")),
            "sample {kind} became durable"
        );
    }
    for kind in &kinds[5..] {
        assert!(records.contains(kind));
    }
    dispatcher.shutdown().await;
    release.send(()).unwrap();
    server.join().unwrap();
}

#[test]
fn continuation_rejects_another_instance_and_reports_evicted_history() {
    child::run(
        "continuation_rejects_another_instance_and_reports_evicted_history",
        run_continuation_rejects_another_instance_and_reports_evicted_history(),
    );
}

async fn run_continuation_rejects_another_instance_and_reports_evicted_history() {
    let source = LiveSource::new("old-core");
    let (url, received, release, server) = spawn_stalled_http_server();
    let mut api = fixture_api(url, None, 1);
    api.dispatcher.replay_batch_size = 1;
    let first = spawn_event_dispatcher(source.clone(), api.clone(), None, options())
        .unwrap()
        .unwrap();
    source.emit(sequenced_event("before-gap", event_type::FLOW_COMPLETED, 1));
    received.recv_timeout(Duration::from_secs(2)).unwrap();
    let continuation = first.shutdown_for_reload().await.unwrap();
    release.send(()).unwrap();
    server.join().unwrap();
    let error = resume_event_dispatcher_with_network(
        LiveSource::new("new-core"),
        api.clone(),
        None,
        options(),
        EventDispatcherNetwork::system(),
        continuation.clone(),
    )
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("different or unidentified core instance"));
    source.emit(sequenced_event("evicted", event_type::FLOW_COMPLETED, 2));
    source.emit(sequenced_event("retained", event_type::FLOW_COMPLETED, 3));
    source.retain_after(2);
    let (url, server) = spawn_scripted_http_server(
        vec![b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"; 2],
    );
    match &mut api.event_sinks[0] {
        EventSinkConfig::Webhook { url: value, .. } => *value = url,
        _ => unreachable!(),
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
    wait_for_sink_status(&second.status_handle(), "receiver", |status| {
        status.replay_gaps > 0
    })
    .await;
    let bodies = server.join().unwrap();
    assert!(bodies.iter().any(|s| s.contains("retained")));
    assert!(bodies.iter().any(|s| s.contains("before-gap")));
    second.shutdown().await;
}

fn fixture_api(url: String, outbox: Option<std::path::PathBuf>, limit: usize) -> ApiConfig {
    let mut api = webhook_api(url, outbox, limit);
    api.dispatcher.exhausted_delivery_policy = zero_config::ExhaustedDeliveryPolicy::Discard;
    api.dispatcher.retry_initial_delay_ms = 10;
    api.dispatcher.retry_max_delay_ms = 20;
    api
}
