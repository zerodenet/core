//! Integration test for `Proxy::probe_outbound_single` — the synchronous,
//! through-proxy single-node latency probe backing the
//! `diagnostics.probe_outbound` command.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

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
