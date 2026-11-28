//! Real native admission exhaustion through the running URLTest refresh path.
use super::*;
use std::error::Error;
use zero_api::{EventStream, RawApiEvent};
use zero_platform_tokio::contract::{ChannelError, Reason};

#[tokio::test]
async fn native_capacity_refresh_preserves_measurements_and_reports_environment_unavailable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_http_probe(&listener).await;
        respond(&mut first).await;
        drop(first);
        let mut held = accept_http_probe(&listener).await;
        // The second actual SOCKS carrier has received HEAD and is suspended
        // reading HTTP: its original and caller ACK cannot have retired.
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        respond(&mut held).await;
    });
    let mut settings = config(port, Some(0));
    // The running keepalive TCP listener consumes one Host original; the held
    // SOCKS carrier consumes the other. Leave role-local capacity available so
    // both diagnostic and policy admission must fail at the shared Host total.
    settings.runtime.native_stream_max_originals = Some(4);
    settings.runtime.carrier_max_total_originals = Some(2);
    let proxy = Proxy::new(settings).unwrap();
    let engine = proxy.engine().clone();
    let events = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .unwrap();
    let diagnostic = proxy.clone();
    let running = proxy.spawn();
    let initial = next_completion(&events).await;
    assert_eq!(initial.payload["terminal_status"], "succeeded");
    let before = member(&engine);
    assert!(before.healthy);
    assert!(before.latency_ms.is_some());
    assert!(before.last_checked_unix_ms.is_some());

    let holder = diagnostic.clone();
    let held = tokio::spawn(async move {
        holder
            .probe_outbound_single("node", "http://probe.example/generate_204")
            .await
    });
    timeout(Duration::from_secs(5), ready_rx)
        .await
        .expect("diagnostic must reach the real HTTP read suspension")
        .unwrap();
    assert!(!held.is_finished());

    // A distinct URL creates a new actual probe: equal target/URL/intent calls
    // deliberately coalesce and would only join the held call as another waiter.
    // Confirm a real typed Capacity while the original HTTP read/ACK is pending.
    let capacity = timeout(
        Duration::from_secs(2),
        diagnostic.probe_outbound_single(
            "node",
            "http://probe.example/generate_204?independent-capacity-check",
        ),
    )
    .await
    .expect("full shared Host total must reject without waiting for the held read")
    .unwrap_err();
    assert_native_capacity(&capacity);
    cool_candidate(&engine);
    let operation = engine
        .trigger_urltest_probe("auto", Some("capacity-refresh"))
        .unwrap();
    let completed = next_completion(&events).await;
    assert_eq!(completed.payload["operation_id"], operation.operation_id);
    assert_eq!(completed.payload["terminal_status"], "inconclusive");
    let reported = &completed.payload["members"][0];
    assert_eq!(reported["error_code"], "environment_unavailable");
    assert_eq!(reported["healthy"], true);
    assert_eq!(reported["latency_ms"].as_u64(), before.latency_ms);
    // The event exposes the unchanged probe diagnostic. Only the retained
    // health history annotates that diagnostic as an inconclusive measurement.
    let wire_error = reported["error"].as_str().unwrap();
    assert!(
        !wire_error.is_empty(),
        "event must retain the probe diagnostic"
    );
    let expected_history_error = format!("inconclusive: {wire_error}");
    let after = member(&engine);
    assert_eq!(after.healthy, before.healthy);
    assert_eq!(after.latency_ms, before.latency_ms);
    assert_eq!(after.last_checked_unix_ms, before.last_checked_unix_ms);
    assert_eq!(
        after.last_error.as_deref(),
        Some(expected_history_error.as_str()),
        "history must retain the complete event diagnostic with its health-only annotation"
    );
    engine
        .check_outbound_health("node")
        .expect_err("preserved healthy measurement must not clear real cooldown");
    assert!(
        !held.is_finished(),
        "refresh must not cancel the unrelated diagnostic"
    );

    release_tx.send(()).unwrap();
    timeout(Duration::from_secs(5), held)
        .await
        .unwrap()
        .unwrap()
        .expect("the original diagnostic must finish using its actual carrier");
    engine
        .check_outbound_health("node")
        .expect_err("successful manual diagnostics remain read-only");
    // Production shutdown is the native close/observer boundary; peer task
    // completion alone is not used as proof of owner retirement.
    running.shutdown().await.unwrap();
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

fn member(engine: &zero_engine::Engine) -> zero_api::PolicyMemberSnapshot {
    engine
        .export_status()
        .config
        .outbound_groups
        .into_iter()
        .find(|group| group.tag == "auto")
        .unwrap()
        .url_test_members
        .into_iter()
        .next()
        .unwrap()
}

async fn next_completion(events: &impl EventStream) -> RawApiEvent {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = events.try_recv() {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual URLTest refresh completion")
}

fn assert_native_capacity(error: &(dyn Error + 'static)) {
    let mut source = Some(error);
    while let Some(error) = source {
        if error
            .downcast_ref::<ChannelError>()
            .is_some_and(|channel| channel.reason == Reason::Capacity)
        {
            return;
        }
        if let Some(zero_engine::EngineError::Io(inner)) =
            error.downcast_ref::<zero_engine::EngineError>()
        {
            source = Some(inner);
        } else if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(|e| e.get_ref())
        {
            source = Some(inner);
        } else {
            source = error.source();
        }
    }
    panic!("native admission error lost its typed ChannelError: {error:?}");
}

async fn accept_http_probe(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut greeting = [0; 3];
    socket.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
    socket.write_all(&[5, 0]).await.unwrap();
    let mut request = [0; 4];
    socket.read_exact(&mut request).await.unwrap();
    assert_eq!(&request[..3], &[5, 1, 0]);
    let address_len = match request[3] {
        1 => 4,
        3 => socket.read_u8().await.unwrap() as usize,
        4 => 16,
        _ => panic!("invalid SOCKS address type"),
    };
    let mut address = vec![0; address_len + 2];
    socket.read_exact(&mut address).await.unwrap();
    socket
        .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();
    let mut http = Vec::new();
    while !http.ends_with(b"\r\n\r\n") {
        assert!(http.len() < 1024);
        http.push(socket.read_u8().await.unwrap());
    }
    assert!(http.starts_with(b"HEAD /generate_204 HTTP/1.1\r\n"));
    socket
}

async fn respond(socket: &mut TcpStream) {
    socket
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
}
