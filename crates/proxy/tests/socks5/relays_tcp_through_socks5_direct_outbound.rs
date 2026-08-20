use super::*;

#[tokio::test]
async fn relays_tcp_through_socks5_direct_outbound() {
    let echo_port = free_port();
    let proxy_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#
    ))
    .expect("parse engine config");

    let engine = Engine::new(config).expect("build engine");
    let probe = engine.clone();
    let engine_handle = spawn_engine(engine);

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

    let request = [
        0x05,
        0x01,
        0x00,
        0x01,
        127,
        0,
        0,
        1,
        ((echo_port >> 8) & 0xff) as u8,
        (echo_port & 0xff) as u8,
    ];
    client.write_all(&request).await.expect("write request");

    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"ping").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");

    drop(client);
    support::wait_for("completed direct flow", || {
        probe.completed_sessions().first().is_some_and(|session| {
            session.inbound_tag.as_deref() == Some("socks-in")
                && session.outcome.kind() == "direct_relayed"
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
    let network = &events[0].payload["record"]["path"]["network"];
    assert_eq!(network["local_address"]["host"], "127.0.0.1");
    assert!(network["local_address"]["port"].as_u64().unwrap() > 0);
    assert_eq!(network["connect_stage"], "connected");
    assert_eq!(network["route_lookup"]["status"], "skipped");
    assert_eq!(network["socket_binding"]["reason"], "loopback");

    engine_handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;
}
