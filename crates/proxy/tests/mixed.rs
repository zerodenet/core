mod support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for_listener};

#[tokio::test]
async fn mixed_inbound_accepts_socks5_and_http_on_same_port() {
    let mixed_port = free_port();
    let socks_echo_port = free_port();
    let http_echo_port = free_port();

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "mixed-in",
                    "listen": {{ "address": "127.0.0.1", "port": {mixed_port} }},
                    "protocol": {{ "type": "mixed" }}
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

    wait_for_listener(mixed_port).await;

    let socks_echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", socks_echo_port))
            .await
            .expect("bind socks echo");
        let (mut stream, _) = listener.accept().await.expect("accept socks echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read socks echo");
        stream.write_all(&buf).await.expect("write socks echo");
    });

    let mut socks_client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy for socks5");
    socks_client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    socks_client
        .read_exact(&mut auth)
        .await
        .expect("read socks auth");
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
        ((socks_echo_port >> 8) & 0xff) as u8,
        (socks_echo_port & 0xff) as u8,
    ];
    socks_client
        .write_all(&request)
        .await
        .expect("write socks request");

    let mut socks_response = [0_u8; 10];
    socks_client
        .read_exact(&mut socks_response)
        .await
        .expect("read socks response");
    assert_eq!(socks_response[1], 0x00);

    socks_client
        .write_all(b"ping")
        .await
        .expect("write socks payload");
    let mut socks_echoed = [0_u8; 4];
    socks_client
        .read_exact(&mut socks_echoed)
        .await
        .expect("read socks payload");
    assert_eq!(&socks_echoed, b"ping");
    drop(socks_client);
    let _ = socks_echo_task.await;

    let http_echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", http_echo_port))
            .await
            .expect("bind http echo");
        let (mut stream, _) = listener.accept().await.expect("accept http echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read http echo");
        stream.write_all(&buf).await.expect("write http echo");
    });

    let mut http_client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy for http");
    let request = format!(
        "CONNECT 127.0.0.1:{http_echo_port} HTTP/1.1\r\nHost: 127.0.0.1:{http_echo_port}\r\n\r\n"
    );
    http_client
        .write_all(request.as_bytes())
        .await
        .expect("write http request");

    let mut http_response = vec![0_u8; 39];
    http_client
        .read_exact(&mut http_response)
        .await
        .expect("read http response");
    assert_eq!(
        &http_response,
        b"HTTP/1.1 200 Connection Established\r\n\r\n"
    );

    http_client
        .write_all(b"pong")
        .await
        .expect("write http payload");
    let mut http_echoed = [0_u8; 4];
    http_client
        .read_exact(&mut http_echoed)
        .await
        .expect("read http payload");
    assert_eq!(&http_echoed, b"pong");

    engine_handle.shutdown().await.expect("shutdown engine");
    let _ = http_echo_task.await;
}

#[tokio::test]
async fn mixed_inbound_routes_absolute_form_get_by_ip_cidr() {
    let mixed_port = free_port();
    let origin_port = free_port();

    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let request = read_http_head(&mut stream).await;
        assert_eq!(
            request,
            format!(
                "GET /router HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes()
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .expect("write origin response");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "mixed-in",
                    "listen": {{ "address": "127.0.0.1", "port": {mixed_port} }},
                    "protocol": {{ "type": "mixed" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [
                    {{
                        "condition": {{
                            "type": "ip_cidr",
                            "values": ["127.0.0.0/8"]
                        }},
                        "action": {{ "type": "direct" }}
                    }}
                ],
                "final": {{ "type": "reject" }}
            }}
        }}"#
    ))
    .expect("parse engine config");

    let engine = Engine::new(config).expect("build engine");
    let engine_handle = spawn_engine(engine);

    wait_for_listener(mixed_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy");
    let request = format!(
        "GET http://127.0.0.1:{origin_port}/router HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\nConnection: close\r\n\r\n"
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("read response");
    assert_eq!(
        response,
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
    );

    engine_handle.shutdown().await.expect("shutdown engine");
    let _ = origin_task.await;
}

async fn read_http_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    while !request.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .await
            .expect("read request head");
        request.push(byte[0]);
    }
    request
}
