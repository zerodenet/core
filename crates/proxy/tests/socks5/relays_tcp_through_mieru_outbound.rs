use super::*;

const USERNAME: &str = "test-user";
const PASSWORD: &str = "test-password";
const PRINCIPAL_KEY: &str = "subscription:mieru-test";

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "mieru"))]
async fn relays_tcp_through_mieru_outbound() {
    let echo_port = free_port();
    let upstream_port = free_port();
    let outer_port = free_port();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-mieru-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "mieru",
                        "users": [
                            {{
                                "username": "{USERNAME}",
                                "password": "{PASSWORD}",
                                "principal_key": "{PRINCIPAL_KEY}"
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
    .expect("parse upstream config");
    let upstream_engine = Engine::new(upstream_config).expect("build upstream engine");
    let upstream_handle = spawn_engine(upstream_engine);

    wait_for_listener(upstream_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "mieru-out",
                    "protocol": {{
                        "type": "mieru",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "username": "{USERNAME}",
                        "password": "{PASSWORD}"
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "mieru-out" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
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

    client.write_all(b"mrtg").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"mrtg");

    drop(client);
    wait_for("completed attributed mieru flow", || {
        !upstream_handle.completed_sessions().is_empty()
    })
    .await;

    let completed = upstream_handle.completed_sessions();
    assert_eq!(
        completed[0]
            .auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_deref()),
        Some(PRINCIPAL_KEY)
    );
    let events = upstream_handle
        .latest(
            usize::MAX,
            EventFilter {
                principal_keys: vec![PRINCIPAL_KEY.to_owned()],
                ..EventFilter::default()
            },
        )
        .expect("read upstream event history");
    let completed_event = events
        .iter()
        .find(|event| event.event_type == event_type::FLOW_COMPLETED)
        .expect("completed Mieru event");
    assert_eq!(
        completed_event.principal_key.as_deref(),
        Some(PRINCIPAL_KEY)
    );

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    upstream_handle
        .shutdown()
        .await
        .expect("shutdown upstream engine");
    let _ = echo_task.await;
}
