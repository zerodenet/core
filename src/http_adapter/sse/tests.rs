use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use zero_api::{EventFilter, EventSource, RawApiEvent};
use zero_config::RuntimeConfig;
use zero_engine::{Engine, EngineHandle};

fn engine() -> EngineHandle {
    EngineHandle::new(
        Engine::new(
            RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn sse_event_bursts_drain_in_order_without_per_event_sleep() {
    let engine = engine();
    let subscriber = engine
        .subscribe(EventFilter {
            event_types: vec!["test.burst".to_owned()],
            ..Default::default()
        })
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(super::run_sse_stream(
        server,
        subscriber,
        Vec::new(),
        Duration::from_secs(30),
        shutdown_rx,
    ));
    for index in 0..128 {
        engine.emit(RawApiEvent::new(
            format!("burst-{index}"),
            "test.burst",
            0,
            json!({"index": index}),
        ));
    }
    let mut client = BufReader::new(client);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut next = 0;
        while next < 128 {
            let mut line = String::new();
            assert!(client.read_line(&mut line).await.unwrap() > 0);
            if let Some(data) = line.strip_prefix("data: ") {
                let event: RawApiEvent = serde_json::from_str(data).unwrap();
                assert_eq!(event.event_type, "test.burst");
                assert_eq!(event.payload["index"], next);
                next += 1;
            }
        }
    })
    .await
    .expect("a ready SSE burst must not be throttled to ten events per second");
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stalled_sse_receiver_does_not_prevent_shutdown() {
    let engine = engine();
    let subscriber = engine.subscribe(EventFilter::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    // Keep the receive window small enough to exercise a blocked write without
    // relying on the host's default TCP buffer size.
    socket2::SockRef::from(&client)
        .set_recv_buffer_size(1024)
        .unwrap();
    socket2::SockRef::from(&server)
        .set_send_buffer_size(1024)
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(super::run_sse_stream(
        server,
        subscriber,
        Vec::new(),
        Duration::from_secs(30),
        shutdown_rx,
    ));
    for index in 0..128 {
        engine.emit(RawApiEvent::new(
            format!("burst-{index}"),
            "test.burst",
            0,
            json!({"padding": "x".repeat(64 * 1024)}),
        ));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!task.is_finished());
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("shutdown must interrupt a stalled SSE write and release its event bridge")
        .unwrap()
        .unwrap();
    drop(client);
}
