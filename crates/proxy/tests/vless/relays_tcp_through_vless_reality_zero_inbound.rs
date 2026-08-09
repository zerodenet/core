use super::*;

#[cfg(feature = "vless")]
#[tokio::test]
async fn relays_tcp_through_vless_reality_zero_inbound() {
    exercise_vless_reality_zero_inbound(false).await;
}

#[cfg(feature = "vless")]
#[tokio::test]
async fn relays_tcp_through_vless_reality_vision_zero_inbound() {
    exercise_vless_reality_zero_inbound(true).await;
}

async fn exercise_vless_reality_zero_inbound(vision: bool) {
    let echo_port = free_port();
    let upstream_port = free_port();
    let outer_port = free_port();
    let _echo = spawn_echo_server(echo_port).await;
    let (private_key, public_key) = vless::reality::generate_reality_key_pair();
    let inbound_flow = if vision {
        r#", "flow": "xtls-rprx-vision""#
    } else {
        ""
    };
    let outbound_flow = if vision {
        r#", "flow": "xtls-rprx-vision""#
    } else {
        ""
    };

    let upstream_config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "vless-reality-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "vless",
                        "users": [{{ "id": "{USER_ID}"{inbound_flow} }}],
                        "reality": {{
                            "private_key": "{private_key}",
                            "short_ids": ["0123456789abcdef"],
                            "server_name": "www.cloudflare.com"
                        }}
                    }}
                }}
            ],
            "outbounds": [
                {{ "tag": "direct", "protocol": {{ "type": "direct" }} }}
            ],
            "route": {{ "final": {{ "type": "route", "outbound": "direct" }} }}
        }}"#
    );
    let upstream_config = RuntimeConfig::parse(&upstream_config).expect("upstream config");
    let upstream_engine = Engine::new(upstream_config).expect("build upstream engine");
    let upstream_handle = spawn_engine(upstream_engine);
    wait_for_listener(upstream_port).await;

    let outer_config = format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vless-reality-chain",
                    "protocol": {{
                        "type": "vless",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "id": "{USER_ID}"{outbound_flow},
                        "reality": {{
                            "public_key": "{public_key}",
                            "short_id": "0123456789abcdef",
                            "server_name": "www.cloudflare.com"
                        }}
                    }}
                }}
            ],
            "route": {{ "final": {{ "type": "route", "outbound": "vless-reality-chain" }} }}
        }}"#
    );
    let outer_config = RuntimeConfig::parse(&outer_config).expect("outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_handle = spawn_engine(outer_engine);
    wait_for_listener(outer_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect socks");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write greeting");
    let mut greeting = [0_u8; 2];
    client
        .read_exact(&mut greeting)
        .await
        .expect("read greeting");
    assert_eq!(greeting, [0x05, 0x00]);

    let mut connect = vec![0x05, 0x01, 0x00, 0x01];
    connect.extend_from_slice(&[127, 0, 0, 1]);
    connect.extend_from_slice(&echo_port.to_be_bytes());
    client.write_all(&connect).await.expect("write connect");
    let mut response = [0_u8; 10];
    client
        .read_exact(&mut response)
        .await
        .expect("read connect response");
    assert_eq!(response[1], 0x00);

    client.write_all(b"ping").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"ping");

    outer_handle.shutdown().await.expect("shutdown outer");
    upstream_handle.shutdown().await.expect("shutdown upstream");
}
