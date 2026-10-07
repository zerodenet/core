#![cfg(feature = "socks5")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use zero_api::{event_type, EventFilter, EventSource};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
async fn successful_policy_probe_restores_a_cooling_candidate() {
    policy_probe_during_cooldown(true).await;
}

#[tokio::test]
async fn failed_policy_probe_keeps_cooldown_and_reports_the_actual_response_failure() {
    policy_probe_during_cooldown(false).await;
}

async fn policy_probe_during_cooldown(respond: bool) {
    let (proxy_port, server) = probe_server(respond).await;
    let inbound = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let inbound_port = inbound.local_addr().unwrap().port();
    drop(inbound);
    let proxy = Proxy::new(config(proxy_port, Some(inbound_port))).unwrap();
    let engine = proxy.engine().clone();
    cool_candidate(&engine);
    let events = engine
        .subscribe(EventFilter {
            event_types: vec![event_type::POLICY_PROBE_COMPLETED.to_owned()],
            ..EventFilter::default()
        })
        .unwrap();
    let running = proxy.spawn();
    let completed = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = events.try_recv() {
                return event;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("policy probe completion");
    running.shutdown().await.unwrap();

    assert_eq!(completed.payload["members"][0]["healthy"], respond);
    timeout(Duration::from_secs(1), server)
        .await
        .expect("URLTest must reach the actual SOCKS proxy during cooldown")
        .unwrap();
    if respond {
        engine
            .check_outbound_health("node")
            .expect("completed successful probe restores candidate eligibility");
    } else {
        assert!(completed.payload["members"][0]["error"]
            .as_str()
            .unwrap()
            .contains("closed without an HTTP response"));
        engine
            .check_outbound_health("node")
            .expect_err("a successful SOCKS handshake is insufficient for recovery");
    }
}

#[tokio::test]
async fn successful_manual_diagnostic_keeps_urltest_observations_read_only() {
    let (port, server) = probe_server(true).await;
    let proxy = Proxy::new(config(port, None)).unwrap();
    let engine = proxy.engine().clone();
    cool_candidate(&engine);
    let before = engine.export_status().config.outbound_groups;

    proxy
        .probe_outbound_single("node", "http://probe.example/generate_204")
        .await
        .expect("manual diagnostics must actually probe a cooling candidate");

    assert_eq!(engine.export_status().config.outbound_groups, before);
    engine
        .check_outbound_health("node")
        .expect_err("manual diagnostics must not alter URLTest eligibility");
    server.await.unwrap();
}

#[tokio::test]
async fn fixed_leaf_and_selector_dial_a_recovered_carrier_without_waiting_for_cooldown() {
    for selector in [false, true] {
        let (port, server) = probe_server(true).await;
        let inbound = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let entry = inbound.local_addr().unwrap().port();
        drop(inbound);
        let mut value = serde_json::to_value(config(port, Some(entry))).unwrap();
        value["outbound_groups"] = if selector {
            serde_json::json!([{"tag":"fixed", "type":"selector", "outbounds":["node"]}])
        } else {
            serde_json::json!([])
        };
        value["route"]["final"]["outbound"] =
            serde_json::json!(if selector { "fixed" } else { "node" });
        let proxy = Proxy::new(serde_json::from_value(value).unwrap()).unwrap();
        let engine = proxy.engine().clone();
        cool_candidate(&engine);
        let before = engine.export_status().config.outbound_groups;
        let running = proxy.spawn();
        let result = timeout(Duration::from_secs(5), async {
            let mut client = loop {
                match TcpStream::connect(("127.0.0.1", entry)).await {
                    Ok(client) => break client,
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            };
            client.write_all(&[5, 1, 0]).await.unwrap();
            let mut greeting = [0; 2];
            client.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 0]);
            let host = b"probe.example";
            let mut request = vec![5, 1, 0, 3, host.len() as u8];
            request.extend_from_slice(host);
            request.extend_from_slice(&80_u16.to_be_bytes());
            client.write_all(&request).await.unwrap();
            let mut accepted = [0; 10];
            client.read_exact(&mut accepted).await.unwrap();
            assert_eq!(accepted[1], 0, "fixed traffic must reach the real carrier");
            client
                .write_all(b"HEAD /generate_204 HTTP/1.1\r\nHost: probe.example\r\n\r\n")
                .await
                .unwrap();
            let expected = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n";
            let mut response = vec![0; expected.len()];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(response, expected);
        })
        .await;
        running.shutdown().await.unwrap();
        result.expect("fixed traffic recovery must not wait for the 60-second cooldown");
        server.await.unwrap();
        engine
            .check_outbound_health("node")
            .expect("real connection restores eligibility");
        assert_eq!(
            engine.export_status().config.outbound_groups,
            before,
            "preserve pinned selection"
        );
    }
}

fn cool_candidate(engine: &zero_engine::Engine) {
    for _ in 0..5 {
        engine.record_outbound_failure("node");
    }
    engine.check_outbound_health("node").unwrap_err();
}

fn config(proxy_port: u16, inbound_port: Option<u16>) -> RuntimeConfig {
    let inbounds = inbound_port
        .map(|port| {
            vec![serde_json::json!({
                "tag": "keepalive", "listen": {"address": "127.0.0.1", "port": port},
                "protocol": {"type": "socks5"}
            })]
        })
        .unwrap_or_default();
    serde_json::from_value(serde_json::json!({
        "inbounds": inbounds,
        "outbounds": [{"tag": "node", "protocol": {
            "type": "socks5", "server": "127.0.0.1", "port": proxy_port
        }}],
        "outbound_groups": [{
            "tag": "auto", "type": "url_test", "outbounds": ["node"],
            "url": "http://probe.example/generate_204", "interval_seconds": 3600
        }],
        "route": {"rules": [], "final": {"type": "route", "outbound": "node"}}
    }))
    .unwrap()
}

async fn probe_server(respond: bool) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut greeting = [0; 2];
        socket.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting[0], 5);
        let mut methods = vec![0; greeting[1] as usize];
        socket.read_exact(&mut methods).await.unwrap();
        assert!(methods.contains(&0));
        socket.write_all(&[5, 0]).await.unwrap();
        let mut connect = [0; 4];
        socket.read_exact(&mut connect).await.unwrap();
        assert_eq!(&connect[..3], &[5, 1, 0]);
        let address_len = match connect[3] {
            1 => 4,
            3 => socket.read_u8().await.unwrap() as usize,
            4 => 16,
            _ => panic!("invalid SOCKS address type"),
        };
        let mut address_and_port = vec![0; address_len + 2];
        socket.read_exact(&mut address_and_port).await.unwrap();
        socket
            .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
            .await
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            assert!(request.len() < 1024, "bounded HTTP probe request");
            request.push(socket.read_u8().await.unwrap());
        }
        assert!(request.starts_with(b"HEAD /generate_204 HTTP/1.1\r\n"));
        if respond {
            socket
                .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        }
    });
    (port, server)
}
