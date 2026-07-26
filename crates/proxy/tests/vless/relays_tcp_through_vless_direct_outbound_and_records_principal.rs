use super::*;

#[tokio::test]
async fn relays_tcp_through_vless_direct_outbound_and_records_principal() {
    let echo_port = free_port();
    let proxy_port = free_port();

    let echo_task = spawn_echo_server(echo_port).await;

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vless-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{
                        "type": "vless",
                        "users": [
                            {{
                                "id": "{USER_ID}",
                                "principal_key": "user:10001"
                            }}
                        ]
                    }}
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
    let engine_handle = spawn_engine(engine);

    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(&vless_request_for_ipv4(USER_ID, [127, 0, 0, 1], echo_port))
        .await
        .expect("write request");

    let mut response = [0_u8; 2];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(response, [0x00, 0x00]);

    client.write_all(b"vles").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"vles");

    drop(client);
    wait_for("completed vless flow", || {
        !engine_handle.completed_sessions().is_empty()
    })
    .await;

    let completed = engine_handle.completed_sessions();
    assert_eq!(completed[0].auth.as_ref().expect("auth").scheme, "vless");
    assert_eq!(
        completed[0]
            .auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_deref()),
        Some("user:10001")
    );

    let events = engine_handle
        .latest(
            usize::MAX,
            EventFilter {
                principal_keys: vec!["user:10001".to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read event history");
    let completed_event = events
        .iter()
        .find(|event| event.event_type == event_type::FLOW_COMPLETED)
        .expect("flow completed event");
    assert_eq!(completed_event.principal_key.as_deref(), Some("user:10001"));

    engine_handle.shutdown().await.expect("shutdown engine");
    echo_task.await.expect("echo task");
}
