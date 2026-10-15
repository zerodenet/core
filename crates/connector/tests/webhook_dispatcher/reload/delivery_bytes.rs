use super::*;

#[test]
fn delivery_bytes_include_inflight_and_preserve_durable_backlog() {
    child::run(
        "delivery_bytes_include_inflight_and_preserve_durable_backlog",
        run(true),
    );
}
#[test]
fn delivery_bytes_backpressure_volatile_facts_without_touching_source_identity() {
    child::run(
        "delivery_bytes_backpressure_volatile_facts_without_touching_source_identity",
        run(false),
    );
}
async fn run(durable: bool) {
    let source = LiveSource::new("byte-core");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.jsonl");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/events", listener.local_addr().unwrap());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut bodies = Vec::new();
        for index in 0..3 {
            let (body, mut stream) = accept_http_request(&listener);
            bodies.push(body);
            if index == 0 {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
        bodies
    });
    let mut api = webhook_api(url, durable.then(|| path.clone()), 4);
    api.dispatcher.max_in_memory_delivery_bytes = 1024;
    api.dispatcher.outbox_min_free_bytes = 1;
    api.dispatcher.outbox_min_free_percent = 0;
    let dispatcher = spawn_event_dispatcher(source.clone(), api, None, options())
        .unwrap()
        .unwrap();
    for sequence in 1..=3 {
        let mut event = sequenced_event(
            &format!("bytes-{sequence}"),
            event_type::FLOW_COMPLETED,
            sequence,
        );
        event.payload = json!({"padding":"x".repeat(4096)});
        source.emit(event);
    }
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    if durable {
        wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
            s.delivery.durable_pending == 3
        })
        .await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let status = dispatcher.sink_status()[0].clone();
    let buffer = status.delivery_buffer.unwrap();
    assert!(status.delivery.in_flight);
    assert_eq!(buffer.byte_budget, 1024);
    assert!(buffer.reserved_bytes > 4096 && buffer.reserved_bytes < 6144);
    assert!(buffer.over_budget);
    if durable {
        assert_eq!(status.pending, 3);
    } else {
        assert_eq!(status.pending, 1);
    }
    release_tx.send(()).unwrap();
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
        s.total_delivered == 3 && s.pending == 0
    })
    .await;
    dispatcher.shutdown().await;
    let bodies = server.join().unwrap();
    for sequence in 1..=3 {
        let event: serde_json::Value =
            serde_json::from_str(request_body(&bodies[sequence as usize - 1])).unwrap();
        assert_eq!(event["event_id"], format!("bytes-{sequence}"));
        assert_eq!(event["core_instance_id"], "byte-core");
    }
    if durable {
        let records = std::fs::read_to_string(&path).unwrap();
        let ack_ids: std::collections::HashSet<String> = records
            .lines()
            .filter_map(|line| {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                (value["op"] == "ack").then(|| value["event_id"].as_str().unwrap().to_owned())
            })
            .collect();
        assert_eq!(ack_ids.len(), 3);
        for sequence in 1..=3 {
            assert!(ack_ids.contains(&format!("bytes-{sequence}")));
        }
    }
}

#[test]
fn older_oversized_durable_event_is_not_starved_by_new_smaller_facts() {
    child::run(
        "older_oversized_durable_event_is_not_starved_by_new_smaller_facts",
        run_order(),
    );
}
async fn run_order() {
    let source = LiveSource::new("byte-order");
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/events", listener.local_addr().unwrap());
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut bodies = Vec::new();
        for index in 0..12 {
            let (request, mut stream) = accept_http_request(&listener);
            bodies.push(request);
            if index == 0 {
                started_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            stream
                .write_all(
                    b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        }
        bodies
    });
    let mut api = webhook_api(url, Some(dir.path().join("outbox.jsonl")), 4);
    api.dispatcher.max_in_memory_delivery_bytes = 1024;
    let dispatcher = spawn_event_dispatcher(source.clone(), api, None, options())
        .unwrap()
        .unwrap();
    source.emit(sequenced_event("order-1", event_type::FLOW_COMPLETED, 1));
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let seed_bytes = dispatcher.sink_status()[0]
        .delivery_buffer
        .unwrap()
        .reserved_bytes;
    assert!(seed_bytes > 0 && seed_bytes < 1024);
    let mut big = sequenced_event("order-2", event_type::FLOW_COMPLETED, 2);
    big.payload = json!({"padding":"x".repeat(4096)});
    source.emit(big);
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
        s.delivery.durable_pending == 2
    })
    .await;
    for sequence in 3..=12 {
        source.emit(sequenced_event(
            &format!("order-{sequence}"),
            event_type::FLOW_COMPLETED,
            sequence,
        ));
    }
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
        s.delivery.durable_pending == 12
    })
    .await;
    assert_eq!(
        dispatcher.sink_status()[0]
            .delivery_buffer
            .unwrap()
            .reserved_bytes,
        seed_bytes
    );
    release_tx.send(()).unwrap();
    wait_for_sink_status(&dispatcher.status_handle(), "receiver", |s| {
        s.total_delivered == 12 && s.pending == 0
    })
    .await;
    dispatcher.shutdown().await;
    let bodies = server.join().unwrap();
    for sequence in 1..=12 {
        let event: serde_json::Value =
            serde_json::from_str(request_body(&bodies[sequence - 1])).unwrap();
        assert_eq!(event["event_id"], format!("order-{sequence}"));
    }
}
