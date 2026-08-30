use super::*;

#[tokio::test]
async fn reports_upstream_connection_failure_to_client_and_session_history() {
    let proxy_port = free_port();
    let unavailable_port = free_port();
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "socks-failure-in",
                "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("failure config");
    let engine = Engine::new(config).expect("build engine");
    let probe = engine.clone();
    let handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write auth");
    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    client
        .write_all(&[
            0x05,
            0x01,
            0x00,
            0x01,
            127,
            0,
            0,
            1,
            (unavailable_port >> 8) as u8,
            unavailable_port as u8,
        ])
        .await
        .expect("write connect request");
    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read failure response");
    assert_eq!(response[0], 0x05);
    assert_eq!(response[1], 0x04);

    support::wait_for("failed session history", || {
        probe.completed_sessions().first().is_some_and(|session| {
            session.inbound_tag.as_deref() == Some("socks-failure-in")
                && session.outcome.kind() == "failed"
                && session.close_reason.as_deref() == Some("upstream_error")
        })
    })
    .await;

    let events = probe
        .latest(
            usize::MAX,
            EventFilter {
                event_types: vec![event_type::FLOW_COMPLETED.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read completed flow events");
    let completed = events.first().expect("completed flow event");
    assert_eq!(completed.payload["record"]["state"], "completed");
    assert_eq!(completed.payload["record"]["result"]["outcome"], "failed");
    assert_eq!(
        completed.payload["record"]["result"]["failure"]["stage"],
        "route_or_establish"
    );
    let network = &completed.payload["record"]["path"]["network"];
    assert_eq!(network["connect_stage"], "connect_socket");
    assert_eq!(network["route_lookup"]["status"], "skipped");
    assert_eq!(network["socket_binding"]["mode"], "system");
    assert_eq!(network["socket_binding"]["reason"], "loopback");
    assert_eq!(network["socket_binding"]["interface_bound"], false);
    let attempts = network["connection_attempts"]
        .as_array()
        .expect("direct TCP connection attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["remote_address"]["host"], "127.0.0.1");
    assert_eq!(
        attempts[0]["remote_address"]["port"],
        u64::from(unavailable_port)
    );
    assert_eq!(attempts[0]["stage"], "connect_socket");
    assert_eq!(attempts[0]["outcome"], "failed");
    assert_eq!(attempts[0]["interface_bound"], false);
    assert!(attempts[0]["error_kind"]
        .as_str()
        .is_some_and(|kind| !kind.is_empty()));
    assert!(attempts[0]["os_error"].as_i64().is_some());
    assert!(attempts[0]["error"]
        .as_str()
        .is_some_and(|error| !error.is_empty()));
    assert!(completed.payload["record"]["result"]["failure"]["code"]
        .as_str()
        .is_some());
    assert!(completed.payload["record"]["result"]["failure"]["message"]
        .as_str()
        .is_some_and(|message| !message.is_empty()));

    handle.shutdown().await.expect("shutdown engine");
}
