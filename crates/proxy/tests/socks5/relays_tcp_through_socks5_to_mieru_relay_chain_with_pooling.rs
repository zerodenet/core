use super::*;

const USERNAME: &str = "pool-user";
const PASSWORD: &str = "pool-password";

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "mieru"))]
async fn relays_tcp_through_socks5_to_mieru_relay_chain_with_pooling() {
    let echo_port = free_port();
    let first_hop_port = free_port();
    let final_hop_port = free_port();
    let outer_port = free_port();
    let release_echo = std::sync::Arc::new(tokio::sync::Notify::new());
    let echo_release = release_echo.clone();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind echo");
        let mut streams = Vec::new();
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.expect("accept echo");
            let mut buf = [0_u8; 4];
            stream.read_exact(&mut buf).await.expect("read echo");
            stream.write_all(&buf).await.expect("write echo");
            streams.push(stream);
        }
        echo_release.notified().await;
        drop(streams);
    });

    let first_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "first-socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {first_hop_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse first hop config");
    let first_hop_engine = Engine::new(first_hop_config).expect("build first hop engine");
    let first_hop_probe = first_hop_engine.clone();
    let first_hop_handle = spawn_engine(first_hop_engine);
    wait_for_listener(first_hop_port).await;

    let final_hop_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "final-mieru-in",
                "listen": {{ "address": "127.0.0.1", "port": {final_hop_port} }},
                "protocol": {{
                    "type": "mieru",
                    "users": [{{ "username": "{USERNAME}", "password": "{PASSWORD}" }}]
                }}
            }}],
            "outbounds": [],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse final hop config");
    let final_hop_engine = Engine::new(final_hop_config).expect("build final hop engine");
    let final_hop_handle = spawn_engine(final_hop_engine);
    wait_for_listener(final_hop_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "outer-socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [
                {{
                    "tag": "first-socks",
                    "protocol": {{
                        "type": "socks5",
                        "server": "127.0.0.1",
                        "port": {first_hop_port}
                    }}
                }},
                {{
                    "tag": "final-mieru",
                    "protocol": {{
                        "type": "mieru",
                        "server": "127.0.0.1",
                        "port": {final_hop_port},
                        "username": "{USERNAME}",
                        "password": "{PASSWORD}"
                    }}
                }}
            ],
            "outbound_groups": [{{
                "tag": "tcp-relay-chain",
                "type": "relay",
                "proxies": ["first-socks", "final-mieru"]
            }}],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "tcp-relay-chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_handle = spawn_engine(outer_engine);
    wait_for_listener(outer_port).await;

    let first = connect_echo(outer_port, echo_port, b"one1").await;
    let second = connect_echo(outer_port, echo_port, b"two2").await;

    wait_for(
        "two logical streams to share one Mieru relay carrier",
        || {
            first_hop_probe
                .active_sessions()
                .iter()
                .filter(|session| {
                    session.network == zero_core::Network::Tcp && session.port == final_hop_port
                })
                .count()
                == 1
        },
    )
    .await;

    release_echo.notify_one();
    drop(first);
    drop(second);
    echo_task.await.expect("join echo task");
    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    final_hop_handle
        .shutdown()
        .await
        .expect("shutdown final hop engine");
    first_hop_handle
        .shutdown()
        .await
        .expect("shutdown first hop engine");
}

async fn connect_echo(outer_port: u16, echo_port: u16, payload: &[u8; 4]) -> TcpStream {
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
            (echo_port >> 8) as u8,
            echo_port as u8,
        ])
        .await
        .expect("write connect request");
    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read connect response");
    assert_eq!(response[1], 0x00);

    client.write_all(payload).await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read echo");
    assert_eq!(&echoed, payload);
    client
}
