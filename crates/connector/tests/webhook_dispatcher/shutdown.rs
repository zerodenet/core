use super::*;

#[test]
fn durable_stalled_webhook_is_cancelled_on_shutdown_and_recovered() {
    const CHILD: &str = "ZERO_TEST_MIXED_DELIVERY_SHUTDOWN";
    if let Some(ready_path) = std::env::var_os(CHILD) {
        std::fs::write(ready_path, b"ready").unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                mixed_delivery_shutdown_and_recovery(false).await;
                mixed_delivery_shutdown_and_recovery(true).await;
                non_durable_facts_finish_after_in_flight_sample().await;
            });
        return;
    }
    // A regressed spawn_blocking dispatcher cannot be aborted by dropping a
    // timed-out future. Keep that regression in an owned, killable test child.
    let directory = tempfile::tempdir().unwrap();
    let ready_path = directory.path().join("ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "durable_stalled_webhook_is_cancelled_on_shutdown_and_recovered",
            "--nocapture",
        ])
        .env(CHILD, &ready_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Process admission on macOS can take longer than test execution. Keep a
    // separate startup allowance without relaxing the shutdown deadlines.
    let startup_deadline = std::time::Instant::now() + Duration::from_secs(180);
    let mut execution_deadline = None;
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        if execution_deadline.is_none() && ready_path.exists() {
            execution_deadline = Some(std::time::Instant::now() + Duration::from_secs(10));
        }
        if std::time::Instant::now() >= execution_deadline.unwrap_or(startup_deadline) {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "mixed-delivery shutdown hung: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

async fn non_durable_facts_finish_after_in_flight_sample() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/events", listener.local_addr().unwrap());
    let (received_tx, received_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (sample, mut stream) = accept_http_request(&listener);
        received_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        let (fact, mut stream) = accept_http_request(&listener);
        stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .unwrap();
        (sample, fact)
    });
    let dispatcher = spawn_event_dispatcher(
        StaticEventSource {
            events: Arc::new(Mutex::new(vec![
                sequenced_event("in-flight-sample", event_type::FLOW_UPDATED, 1),
                sequenced_event("queued-lifecycle-fact", event_type::FLOW_COMPLETED, 2),
            ])),
        },
        webhook_api(url, None, 8),
        None,
        EventDispatcherOptions {
            poll_interval: Duration::from_millis(10),
            max_retry_attempts: 1,
        },
    )
    .unwrap()
    .unwrap();
    received_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |sink| {
        sink.delivery.in_flight && sink.pending == 2
    })
    .await;
    let release = tokio::spawn(async move {
        // The fact must keep its delivery channel after shutdown starts.
        tokio::time::sleep(Duration::from_millis(100)).await;
        release_tx.send(()).unwrap();
    });
    tokio::time::timeout(Duration::from_secs(2), dispatcher.shutdown())
        .await
        .expect("non-durable lifecycle facts must still be delivered");
    release.await.unwrap();
    let (sample, fact) = server.join().unwrap();
    assert!(request_body(&sample).contains("in-flight-sample"));
    assert!(request_body(&fact).contains("queued-lifecycle-fact"));
}

async fn mixed_delivery_shutdown_and_recovery(sample_in_flight: bool) {
    let outbox_path = temp_path("zero-connector-cancelled-webhook-outbox.jsonl");
    let _ = std::fs::remove_file(&outbox_path);
    let (stalled_url, stalled_received, release_stalled, stalled_server) =
        spawn_stalled_http_server();
    let event = sequenced_event("event-cancelled-durable", event_type::FLOW_COMPLETED, 1);
    let api = webhook_api(stalled_url, Some(outbox_path.clone()), 8);
    let sample = sequenced_event(
        "queued-discardable-sample",
        event_type::FLOW_UPDATED,
        if sample_in_flight { 1 } else { 2 },
    );
    let mut event = event;
    event.sequence = Some(if sample_in_flight { 2 } else { 1 });
    let events = if sample_in_flight {
        vec![sample, event]
    } else {
        vec![event, sample]
    };
    let replay_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatcher = spawn_event_dispatcher(
        CountingEventSource {
            events: Arc::new(Mutex::new(events)),
            replay_calls: replay_calls.clone(),
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
    let status = dispatcher.status_handle();

    stalled_received
        .recv_timeout(Duration::from_secs(2))
        .expect("stalled webhook received durable delivery");
    let stalled = wait_for_sink_status(&status, "receiver", |sink| {
        sink.delivery.in_flight && sink.delivery.durable_pending == 1
    })
    .await;
    assert_eq!(stalled.pending, 1); // Outbox counts durable facts only.
    tokio::time::timeout(Duration::from_secs(2), async {
        while replay_calls.load(std::sync::atomic::Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("source did not process the queued sample");
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
