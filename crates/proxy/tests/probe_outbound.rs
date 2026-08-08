//! Integration test for `Proxy::probe_outbound_single` — the synchronous,
//! through-proxy single-node latency probe backing the
//! `diagnostics.probe_outbound` command.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use zero_api::{
    event_type, CommandRequest, CommandService, DiagnosticsProbeOutboundCommand, EventFilter,
    EventSource,
};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[tokio::test]
async fn manual_policy_probe_coalesces_with_a_running_scheduled_cycle() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controlled probe server");
    let port = listener.local_addr().expect("local_addr").port();
    let inbound_reservation = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve inbound port");
    let inbound_port = inbound_reservation
        .local_addr()
        .expect("inbound local_addr")
        .port();
    drop(inbound_reservation);
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, mut release_rx) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("accept probe");
            let mut buf = [0_u8; 512];
            let _ = socket.read(&mut buf).await;
            accepted_tx.send(()).expect("report accepted probe");
            if release_rx.recv().await.is_none() {
                break;
            }
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await
                .expect("write controlled response");
        }
    });
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "test-in",
                "listen": {{ "address": "127.0.0.1", "port": {inbound_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [{{ "tag": "direct", "protocol": {{ "type": "direct" }} }}],
            "outbound_groups": [{{
                "tag": "auto",
                "type": "url_test",
                "outbounds": ["direct"],
                "url": "http://127.0.0.1:{port}/generate_204",
                "interval_seconds": 1
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse urltest config");
    let proxy = Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let subscriber = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .expect("subscribe to policy probes");
    let running = proxy.spawn();

    timeout(std::time::Duration::from_secs(5), accepted_rx.recv())
        .await
        .expect("startup probe connection")
        .expect("startup probe report");
    release_tx.send(()).expect("release startup probe");
    wait_for_policy_probe(&subscriber, "startup").await;

    timeout(std::time::Duration::from_secs(5), accepted_rx.recv())
        .await
        .expect("scheduled probe connection")
        .expect("scheduled probe report");
    let ack = engine
        .trigger_urltest_probe("auto", Some("manual-during-scheduled"))
        .expect("trigger during scheduled probe");
    assert!(ack.coalesced);
    assert_ne!(ack.operation_id, "manual-during-scheduled");
    release_tx.send(()).expect("release scheduled probe");
    let completed = wait_for_policy_probe(&subscriber, "scheduled").await;
    assert_eq!(completed.payload["operation_id"], ack.operation_id);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    while let Some(event) = subscriber.try_recv() {
        assert_ne!(event.payload["trigger"], "manual");
    }

    running.shutdown().await.expect("shutdown proxy");
    server.abort();
}

async fn wait_for_policy_probe(
    subscriber: &zero_engine::EventSubscriber,
    trigger: &str,
) -> zero_api::RawApiEvent {
    timeout(std::time::Duration::from_secs(5), async {
        loop {
            if let Some(event) = subscriber.try_recv() {
                if event.payload["trigger"] == trigger {
                    return event;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("policy probe completion")
}

#[tokio::test(flavor = "multi_thread")]
async fn diagnostics_probe_outbound_echoes_operation_and_generation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("local_addr").port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept probe");
        let mut buf = [0_u8; 512];
        let _ = socket.read(&mut buf).await;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("write response");
    });
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy);
    let url = format!("http://127.0.0.1:{port}/generate_204");

    let response = tokio::task::block_in_place(|| {
        handle.execute(CommandRequest::DiagnosticsProbeOutbound(
            DiagnosticsProbeOutboundCommand {
                target_tag: "direct".to_owned(),
                url: Some(url),
                operation_id: Some("diagnostic-1".to_owned()),
            },
        ))
    })
    .expect("execute outbound diagnostic");
    let result = response.result.expect("diagnostic result");
    assert_eq!(result["operation_id"], "diagnostic-1");
    assert_eq!(result["core_instance_id"], engine.core_instance_id());
    assert_eq!(result["config_revision"], 1);
    assert_eq!(result["terminal_status"], "succeeded");
    assert_eq!(result["reachable"], true);
    assert!(result["completed_at_unix_ms"].as_u64().is_some());
    server.await.expect("probe server task");
}

/// A probe through a `direct` outbound must reach the target via the real proxy
/// dispatch path (TLS-less here since the URL is plain HTTP) and report a
/// sub-timeout latency. This is the single-node counterpart to the async
/// `url_test` group probe.
#[tokio::test]
async fn probe_outbound_single_measures_through_proxy_latency() {
    // Minimal HTTP/204 server on an ephemeral port.
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("local_addr").port();
    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        // Drain the HEAD request (best-effort), then reply 204.
        let mut buf = [0u8; 512];
        let _ = sock.read(&mut buf).await;
        sock.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("write 204");
    });

    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [
                { "tag": "probe-target", "protocol": { "type": "direct" } }
            ],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");

    let url = format!("http://127.0.0.1:{port}/generate_204");
    let latency_ms = timeout(
        std::time::Duration::from_secs(5),
        proxy.probe_outbound_single("probe-target", &url),
    )
    .await
    .expect("probe_outbound_single did not complete within 5s")
    .expect("probe through direct outbound should succeed");

    // localhost RTT must be well under the 5s probe timeout.
    assert!(
        latency_ms < 5_000,
        "expected sub-timeout localhost latency, got {latency_ms} ms"
    );
    let _ = server.await;
}

/// Probing a tag that does not exist must surface a not-found error rather
/// than panicking or hanging.
#[tokio::test]
async fn probe_outbound_single_errors_on_unknown_tag() {
    let config = RuntimeConfig::parse(
        r#"{
            "inbounds": [],
            "outbounds": [{ "tag": "direct-out", "protocol": { "type": "direct" } }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");

    let result = proxy
        .probe_outbound_single("no-such-tag", "http://127.0.0.1:1/")
        .await;
    assert!(result.is_err(), "probing an unknown tag should error");
}

#[tokio::test]
async fn probe_outbound_single_uses_new_snapshot_immediately_after_reload() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept probe");
        let mut buf = [0_u8; 512];
        let _ = socket.read(&mut buf).await;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .expect("write response");
    });

    let proxy = Proxy::new(config_with_direct_outbounds(3, None, None)).expect("build proxy");
    proxy
        .engine()
        .reload_runtime_config(config_with_direct_outbounds(35, None, None))
        .expect("reload larger config");

    let url = format!("http://127.0.0.1:{port}/generate_204");
    proxy
        .probe_outbound_single("node-34", &url)
        .await
        .expect("new outbound should be probeable immediately");
    server.await.expect("probe server task");
}

#[tokio::test]
async fn automatic_urltest_rebuilds_runtime_across_large_and_small_reloads() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("local addr").port();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.expect("accept probe");
            tokio::spawn(async move {
                let mut buf = [0_u8; 512];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    let url = format!("http://127.0.0.1:{port}/generate_204");

    let inbound_probe = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve inbound port");
    let inbound_port = inbound_probe.local_addr().expect("inbound addr").port();
    drop(inbound_probe);

    let proxy = Proxy::new(config_with_direct_outbounds(
        3,
        Some(&url),
        Some(inbound_port),
    ))
    .expect("build proxy");
    let running = proxy.spawn();
    wait_for_urltest_members(running.engine(), 3).await;

    running
        .engine()
        .reload_runtime_config(config_with_direct_outbounds(
            35,
            Some(&url),
            Some(inbound_port),
        ))
        .expect("reload larger config");
    wait_for_urltest_members(running.engine(), 35).await;

    running
        .engine()
        .reload_runtime_config(config_with_direct_outbounds(
            3,
            Some(&url),
            Some(inbound_port),
        ))
        .expect("reload smaller config");
    wait_for_urltest_members(running.engine(), 3).await;

    running.shutdown().await.expect("shutdown proxy");
    server.abort();
}

#[tokio::test]
async fn reload_during_urltest_cancels_old_generation_and_starts_new_generation() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe server");
    let port = listener.local_addr().expect("local addr").port();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut accepted_tx = Some(accepted_tx);
        loop {
            let (mut socket, _) = listener.accept().await.expect("accept probe");
            if let Some(accepted_tx) = accepted_tx.take() {
                let _ = accepted_tx.send(());
            }
            tokio::spawn(async move {
                let mut buf = [0_u8; 512];
                let _ = socket.read(&mut buf).await;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                    .await;
            });
        }
    });
    let url = format!("http://127.0.0.1:{port}/generate_204");
    let inbound_probe = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve inbound port");
    let inbound_port = inbound_probe.local_addr().expect("inbound addr").port();
    drop(inbound_probe);

    let proxy = Proxy::new(config_with_direct_outbounds(
        35,
        Some(&url),
        Some(inbound_port),
    ))
    .expect("build proxy");
    let running = proxy.spawn();
    timeout(std::time::Duration::from_secs(5), accepted_rx)
        .await
        .expect("urltest did not start")
        .expect("probe server stopped before first accept");

    running
        .engine()
        .reload_runtime_config(config_with_direct_outbounds(
            3,
            Some(&url),
            Some(inbound_port),
        ))
        .expect("reload while probe is active");
    wait_for_urltest_members(running.engine(), 3).await;

    running.shutdown().await.expect("shutdown proxy");
    server.abort();
}

async fn wait_for_urltest_members(engine: &zero_engine::Engine, expected: usize) {
    timeout(std::time::Duration::from_secs(10), async {
        loop {
            let status = engine.export_status();
            let members = status
                .config
                .outbound_groups
                .iter()
                .find(|group| group.tag == "auto")
                .map(|group| group.url_test_members.len());
            if members == Some(expected) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("urltest did not publish {expected} member results"));
}

fn config_with_direct_outbounds(
    count: usize,
    urltest_url: Option<&str>,
    inbound_port: Option<u16>,
) -> RuntimeConfig {
    let outbounds = (0..count)
        .map(|index| {
            serde_json::json!({
                "tag": format!("node-{index}"),
                "protocol": { "type": "direct" }
            })
        })
        .collect::<Vec<_>>();
    let groups = urltest_url
        .map(|url| {
            vec![serde_json::json!({
                "tag": "auto",
                "type": "url_test",
                "outbounds": (0..count).map(|index| format!("node-{index}")).collect::<Vec<_>>(),
                "url": url,
                "interval_seconds": 3600
            })]
        })
        .unwrap_or_default();
    let inbounds = inbound_port
        .map(|port| {
            vec![serde_json::json!({
                "tag": "keepalive",
                "listen": { "address": "127.0.0.1", "port": port },
                "protocol": { "type": "direct" }
            })]
        })
        .unwrap_or_default();
    RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": inbounds,
            "outbounds": outbounds,
            "outbound_groups": groups,
            "route": { "rules": [], "final": { "type": "direct" } }
        })
        .to_string(),
    )
    .expect("parse generated config")
}
