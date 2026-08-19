mod support;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_core::Address;
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
    let http_source = http_client.local_addr().expect("HTTP client source");
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
    let active = engine_handle.active_sessions();
    assert_eq!(
        active.len(),
        1,
        "one accepted HTTP CONNECT socket must create one active flow"
    );
    assert_eq!(active[0].source_ip, Some(Address::Ipv4([127, 0, 0, 1])));
    assert_eq!(active[0].source_port, Some(http_source.port()));
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    assert_eq!(active[0].process_id, Some(std::process::id()));

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
                            "type": "ip",
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

#[tokio::test]
async fn mixed_http_branch_reparses_persistent_requests() {
    let mixed_port = free_port();
    let origin_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        for path in ["/first", "/second"] {
            let (mut stream, _) = listener.accept().await.expect("accept origin");
            let request = String::from_utf8(read_http_head(&mut stream).await).expect("request");
            assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .expect("response");
        }
    });
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"mixed-in","listen":{{"address":"127.0.0.1","port":{mixed_port}}},"protocol":{{"type":"mixed"}}}}],
            "outbounds":[],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#
    ))
    .expect("parse config");
    let engine_handle = spawn_engine(Engine::new(config).expect("build engine"));
    wait_for_listener(mixed_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy");
    for path in ["/first", "/second"] {
        client
            .write_all(
                format!("GET http://127.0.0.1:{origin_port}{path} HTTP/1.1\r\nHost: ignored\r\nProxy-Connection: keep-alive\r\n\r\n").as_bytes(),
            )
            .await
            .expect("request");
        let head = read_http_head(&mut client).await;
        let length = String::from_utf8_lossy(&head)
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .map(str::to_owned)
            })
            .expect("content length")
            .parse::<usize>()
            .expect("content length");
        let mut body = vec![0_u8; length];
        client.read_exact(&mut body).await.expect("body");
        assert_eq!(body, b"ok");
    }

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn mixed_http_branch_relays_tiny_fixed_length_post_body() {
    let mixed_port = free_port();
    let origin_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let request = String::from_utf8(read_http_head(&mut stream).await).expect("request");
        assert!(request.starts_with("POST /api/v1/nodes/4/diagnostics HTTP/1.1\r\n"));
        assert!(request.contains("Content-Length: 2\r\n"));
        assert!(!request.to_ascii_lowercase().contains("proxy-connection:"));
        let mut body = [0_u8; 2];
        tokio::time::timeout(Duration::from_millis(100), stream.read_exact(&mut body))
            .await
            .expect("request body must arrive with the forwarded head")
            .expect("request body");
        assert_eq!(&body, b"{}");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
            .await
            .expect("response");
    });
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"mixed-in","listen":{{"address":"127.0.0.1","port":{mixed_port}}},"protocol":{{"type":"mixed"}}}}],
            "outbounds":[],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#
    ))
    .expect("parse config");
    let engine_handle = spawn_engine(Engine::new(config).expect("build engine"));
    wait_for_listener(mixed_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy");
    let request_head = format!(
        "POST http://127.0.0.1:{origin_port}/api/v1/nodes/4/diagnostics HTTP/1.1\r\nHost: ignored\r\nProxy-Connection: keep-alive\r\nContent-Length: 2\r\n\r\n"
    );
    client
        .write_all(request_head.as_bytes())
        .await
        .expect("request head");
    tokio::time::sleep(Duration::from_millis(500)).await;
    client.write_all(b"{}").await.expect("request body");
    let response = read_http_head(&mut client).await;
    assert_eq!(response, b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n");
    let mut body = [0_u8; 2];
    client.read_exact(&mut body).await.expect("response body");
    assert_eq!(&body, b"OK");

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn mixed_http_redirect_skips_non_redirect_rewrite_rules() {
    let mixed_port = free_port();
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"mixed-in","listen":{{"address":"127.0.0.1","port":{mixed_port}}},"protocol":{{"type":"mixed"}}}}],
            "outbounds":[],
            "route":{{
                "rules":[],
                "url_rewrite":[
                    {{"from":"a.example","to":"b.example"}},
                    {{"from":"c.example","to":"d.example","status_code":302}}
                ],
                "final":{{"type":"reject"}}
            }}
        }}"#
    ))
    .expect("parse config");
    let engine_handle = spawn_engine(Engine::new(config).expect("build engine"));
    wait_for_listener(mixed_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", mixed_port))
        .await
        .expect("connect mixed proxy");
    client
        .write_all(b"GET http://c.example/path HTTP/1.1\r\nHost: c.example\r\n\r\n")
        .await
        .expect("request");
    let head = read_http_head(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 302 Found\r\n"));
    assert!(String::from_utf8_lossy(&head).contains("Location: https://d.example:80\r\n"));

    engine_handle.shutdown().await.expect("shutdown engine");
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
