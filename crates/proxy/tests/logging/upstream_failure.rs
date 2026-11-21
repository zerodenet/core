use super::*;

#[tokio::test]
async fn logs_selected_leaf_and_actual_dial_address_when_selector_node_refuses_connection() {
    let buffer = init_test_tracing();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_port = listener.local_addr().unwrap().port();
    // This is deliberately an absent receiver, not a listener worker. Darwin
    // parks SYNs to a retained bound socket instead of returning refusal.
    drop(listener);
    let proxy_port = free_port();
    let config = RuntimeConfig::parse(&format!(r#"{{
        "inbounds": [{{"tag":"diagnostic-in", "listen":{{"address":"127.0.0.1","port":{proxy_port}}},
            "protocol":{{"type":"socks5"}}}}],
        "outbounds": [{{"tag":"diagnostic-ss-node", "protocol":{{"type":"shadowsocks", "server":"127.0.0.1",
            "port":{node_port}, "cipher":"chacha20-ietf-poly1305", "password":"diagnostic-secret-not-for-logs"}}}}],
        "outbound_groups": [{{"tag":"diagnostic-selector", "type":"selector", "outbounds":["diagnostic-ss-node"], "selected":"diagnostic-ss-node"}}],
        "route":{{"rules":[], "final":{{"type":"route","outbound":"diagnostic-selector"}}}}
    }}"#)).expect("diagnostic config");
    let engine = Engine::new(config).unwrap();
    let probe = engine.clone();
    let handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;
    // Both attempts must retain their actual native refusal and failure facts.
    for expected_completed in 1..=2 {
        let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
            .await
            .unwrap_or_else(|source| {
                let logs = String::from_utf8_lossy(&buffer.lock().unwrap()).into_owned();
                panic!("request {expected_completed}: {source}; original host log:\n{logs}")
            });
        client.write_all(&[5, 1, 0]).await.unwrap();
        let mut auth = [0; 2];
        client.read_exact(&mut auth).await.unwrap();
        client
            .write_all(&[5, 1, 0, 1, 192, 0, 2, 100, 1, 187])
            .await
            .unwrap();
        let mut response = [0; 10];
        client.read_exact(&mut response).await.unwrap();
        assert_ne!(response[1], 0);
        wait_for("diagnostic failure record", || {
            probe
                .completed_sessions()
                .iter()
                .filter(|record| record.inbound_tag.as_deref() == Some("diagnostic-in"))
                .count()
                == expected_completed
        })
        .await;
    }
    handle.shutdown().await.unwrap();
    let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    let failure = logs
        .lines()
        .find(|line| line.contains("session failed") && line.contains("diagnostic-in"))
        .expect("session failure warning at INFO logging level");
    for expected in [
        "outbound_tag=\"diagnostic-ss-node\"",
        "outbound_protocol=\"shadowsocks\"",
        "dial_ip=\"127.0.0.1\"",
        "stage=\"connect_socket\"",
        "upstream_server=\"127.0.0.1\"",
        "os_error=",
    ] {
        assert!(failure.contains(expected), "missing {expected}: {failure}");
    }
    assert!(
        failure.contains(&format!("dial_port={node_port}")),
        "{failure}"
    );
    let native_code: i32 = failure
        .split("os_error=")
        .nth(1)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        io::Error::from_raw_os_error(native_code).kind(),
        io::ErrorKind::ConnectionRefused,
        "an occupied or filtered endpoint cannot substitute for native refusal"
    );
    assert!(!logs.contains("diagnostic-secret-not-for-logs"));
    let records = probe.completed_sessions();
    let record = records
        .iter()
        .find(|record| record.inbound_tag.as_deref() == Some("diagnostic-in"))
        .unwrap();
    assert_eq!(record.outbound_tag.as_deref(), Some("diagnostic-ss-node"));
    assert_eq!(record.failure.as_ref().unwrap().stage, "connect_socket");
}
