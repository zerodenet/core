mod support;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for, wait_for_listener};

#[tokio::test]
async fn relays_tcp_through_http_direct_outbound() {
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
                    "tag": "http-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "http" }}
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
    let request =
        format!("CONNECT 127.0.0.1:{echo_port} HTTP/1.1\r\nHost: 127.0.0.1:{echo_port}\r\n\r\n");
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let mut response = vec![0_u8; 39];
    client
        .read_exact(&mut response)
        .await
        .expect("read response");
    assert_eq!(&response, b"HTTP/1.1 200 Connection Established\r\n\r\n");

    client.write_all(b"pong").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"pong");

    engine_handle.shutdown().await.expect("shutdown engine");
    let _ = echo_task.await;
}

#[tokio::test]
async fn relays_absolute_form_get_through_http_direct_outbound() {
    let origin_port = free_port();
    let proxy_port = free_port();

    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let request = read_http_head(&mut stream).await;
        assert_eq!(
            request,
            format!(
                "GET /status?view=full HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes()
        );
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong")
            .await
            .expect("write origin response");
    });

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "http-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "http" }}
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
    let request = format!(
        "GET http://127.0.0.1:{origin_port}/status?view=full HTTP/1.1\r\nHost: 127.0.0.1:{origin_port}\r\nConnection: close\r\n\r\n"
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
        b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\npong"
    );

    engine_handle.shutdown().await.expect("shutdown engine");
    let _ = origin_task.await;
}

#[tokio::test]
async fn rejects_http_blocked_domain_via_route_rule() {
    let proxy_port = free_port();

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "http-in",
                    "listen": {{ "address": "127.0.0.1", "port": {proxy_port} }},
                    "protocol": {{ "type": "http" }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [
                    {{
                        "condition": {{
                            "type": "domain",
                            "values": ["blocked.example"]
                        }},
                        "action": {{ "type": "reject" }}
                    }}
                ],
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
    let request = "CONNECT blocked.example:443 HTTP/1.1\r\nHost: blocked.example:443\r\n\r\n";
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let mut response = vec![0_u8; 64];
    let read = client.read(&mut response).await.expect("read response");
    let response = &response[..read];
    assert_eq!(
        response,
        b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );

    engine_handle.shutdown().await.expect("shutdown engine");
}

#[tokio::test]
async fn reparses_vite_like_requests_on_one_persistent_proxy_connection() {
    let origin_port = free_port();
    let proxy_port = free_port();

    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        for (path, body) in [("/", "index"), ("/@vite/client", "module")] {
            let (mut stream, _) = listener.accept().await.expect("accept origin");
            let request = String::from_utf8(read_http_head(&mut stream).await).expect("request");
            assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            assert!(request.contains(&format!("Host: 127.0.0.1:{origin_port}\r\n")));
            assert!(!request.to_ascii_lowercase().contains("proxy-connection"));
            assert!(!request.to_ascii_lowercase().contains("proxy-authorization"));
            assert!(!request.contains("X-Remove:"));
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("write response");
        }
    });

    let engine =
        Engine::new(http_config(proxy_port, "[]", r#"{"type":"direct"}"#)).expect("build engine");
    let engine_handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    for (path, expected) in [
        ("/", b"index".as_slice()),
        ("/@vite/client", b"module".as_slice()),
    ] {
        let request = format!(
            "GET http://127.0.0.1:{origin_port}{path} HTTP/1.1\r\nHost: wrong.example\r\nProxy-Connection: Keep-Alive\r\nProxy-Authorization: Basic c2VjcmV0\r\nConnection: X-Remove\r\nX-Remove: secret\r\n\r\n"
        );
        client.write_all(request.as_bytes()).await.expect("request");
        let (_, body) = read_http_response(&mut client).await;
        assert_eq!(body, expected);
    }

    wait_for("two request-level HTTP sessions", || {
        engine_handle.completed_sessions().len() >= 2
    })
    .await;
    let completed = engine_handle.completed_sessions();
    assert_ne!(completed[0].id, completed[1].id);
    assert!(completed.iter().take(2).all(|session| {
        session.inbound_tag.as_deref() == Some("http-in")
            && session.outbound_tag.as_deref() == Some("direct")
            && session.bytes_up > 0
            && session.bytes_down > 0
    }));

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn later_persistent_request_is_routed_and_blocked_independently() {
    let origin_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let _ = read_http_head(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .expect("response");
    });
    let rules = r#"[{"condition":{"type":"domain","values":["blocked.example"]},"action":{"type":"reject"}}]"#;
    let engine =
        Engine::new(http_config(proxy_port, rules, r#"{"type":"direct"}"#)).expect("build engine");
    let engine_handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/first HTTP/1.1\r\nHost: ignored\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("first request");
    let (_, first) = read_http_response(&mut client).await;
    assert_eq!(first, b"ok");

    client
        .write_all(b"GET http://blocked.example/second HTTP/1.1\r\nHost: blocked.example\r\n\r\n")
        .await
        .expect("second request");
    let (head, body) = read_http_response(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 403 Forbidden\r\n"));
    assert!(body.is_empty());

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn relays_fixed_and_chunked_messages_then_keeps_parsing_requests() {
    let origin_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut first, _) = listener.accept().await.expect("accept first");
        let head = String::from_utf8(read_http_head(&mut first).await).expect("head");
        assert!(head.starts_with("POST /upload HTTP/1.1\r\n"));
        let mut body = [0_u8; 2];
        first.read_exact(&mut body).await.expect("request body");
        assert_eq!(&body, b"{}");
        first
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\npong\r\n0\r\n\r\n")
            .await
            .expect("chunked response");

        let (mut second, _) = listener.accept().await.expect("accept second");
        let head = String::from_utf8(read_http_head(&mut second).await).expect("head");
        assert!(head.starts_with("GET /after HTTP/1.1\r\n"));
        second
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .expect("empty response");
    });
    let engine =
        Engine::new(http_config(proxy_port, "[]", r#"{"type":"direct"}"#)).expect("build engine");
    let engine_handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("POST http://127.0.0.1:{origin_port}/upload HTTP/1.1\r\nHost: ignored\r\nProxy-Connection: keep-alive\r\nContent-Length: 2\r\n\r\n{{}}").as_bytes(),
        )
        .await
        .expect("post");
    let (head, body) = read_http_response(&mut client).await;
    assert!(String::from_utf8_lossy(&head)
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked"));
    assert_eq!(body, b"4\r\npong\r\n0\r\n\r\n");

    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/after HTTP/1.1\r\nHost: ignored\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("second request");
    let (head, body) = read_http_response(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 204 No Content\r\n"));
    assert!(body.is_empty());

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn switches_to_raw_relay_only_after_successful_upgrade() {
    let origin_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let head = String::from_utf8(read_http_head(&mut stream).await).expect("head");
        assert!(head.contains("Connection: Upgrade\r\n"));
        assert!(head.contains("Upgrade: websocket\r\n"));
        stream
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
            .await
            .expect("upgrade response");
        let mut payload = [0_u8; 4];
        stream.read_exact(&mut payload).await.expect("raw payload");
        stream.write_all(&payload).await.expect("raw response");
    });
    let engine =
        Engine::new(http_config(proxy_port, "[]", r#"{"type":"direct"}"#)).expect("build engine");
    let engine_handle = spawn_engine(engine);
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/socket HTTP/1.1\r\nHost: ignored\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").as_bytes(),
        )
        .await
        .expect("upgrade request");
    let head = read_http_head(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 101 Switching Protocols\r\n"));
    client.write_all(b"ping").await.expect("raw payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("raw response");
    assert_eq!(&echoed, b"ping");

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn failed_upgrade_response_returns_to_http_request_parsing() {
    let origin_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        for (path, status, body) in [
            ("/socket", "400 Bad Request", "no"),
            ("/after", "200 OK", "yes"),
        ] {
            let (mut stream, _) = listener.accept().await.expect("accept origin");
            let head = String::from_utf8(read_http_head(&mut stream).await).expect("head");
            assert!(head.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .expect("response");
        }
    });
    let engine_handle = spawn_engine(
        Engine::new(http_config(proxy_port, "[]", r#"{"type":"direct"}"#)).expect("build engine"),
    );
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/socket HTTP/1.1\r\nHost: ignored\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").as_bytes(),
        )
        .await
        .expect("upgrade request");
    let (head, body) = read_http_response(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 400 Bad Request\r\n"));
    assert_eq!(body, b"no");

    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/after HTTP/1.1\r\nHost: ignored\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("request after failed upgrade");
    let (head, body) = read_http_response(&mut client).await;
    assert!(head.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert_eq!(body, b"yes");

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn close_delimited_response_is_reframed_for_persistent_client() {
    let origin_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        for (path, body) in [("/legacy", "legacy"), ("/after", "next")] {
            let (mut stream, _) = listener.accept().await.expect("accept origin");
            let head = String::from_utf8(read_http_head(&mut stream).await).expect("head");
            assert!(head.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            if path == "/legacy" {
                stream
                    .write_all(format!("HTTP/1.0 200 OK\r\n\r\n{body}").as_bytes())
                    .await
                    .expect("legacy response");
            } else {
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .expect("response");
            }
        }
    });
    let engine_handle = spawn_engine(
        Engine::new(http_config(proxy_port, "[]", r#"{"type":"direct"}"#)).expect("build engine"),
    );
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/legacy HTTP/1.1\r\nHost: ignored\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("legacy request");
    let (head, body) = read_http_response(&mut client).await;
    assert!(String::from_utf8_lossy(&head)
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked\r\n"));
    assert_eq!(body, b"6\r\nlegacy\r\n0\r\n\r\n");

    client
        .write_all(
            format!("GET http://127.0.0.1:{origin_port}/after HTTP/1.1\r\nHost: ignored\r\n\r\n")
                .as_bytes(),
        )
        .await
        .expect("request after legacy response");
    let (_, body) = read_http_response(&mut client).await;
    assert_eq!(body, b"next");

    engine_handle.shutdown().await.expect("shutdown engine");
    origin_task.await.expect("origin task");
}

#[tokio::test]
async fn routes_plain_http_post_through_a_configured_proxy_outbound() {
    let origin_port = free_port();
    let upstream_port = free_port();
    let proxy_port = free_port();
    let origin_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", origin_port))
            .await
            .expect("bind origin");
        let (mut stream, _) = listener.accept().await.expect("accept origin");
        let head = String::from_utf8(read_http_head(&mut stream).await).expect("head");
        assert!(head.starts_with("POST /proxied HTTP/1.1\r\n"));
        assert!(head.contains("Content-Length: 2\r\n"));
        assert!(!head.to_ascii_lowercase().contains("proxy-connection:"));
        let mut body = [0_u8; 2];
        stream.read_exact(&mut body).await.expect("request body");
        assert_eq!(&body, b"{}");
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\npong\r\n0\r\n\r\n")
            .await
            .expect("response");
    });
    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"upstream","listen":{{"address":"127.0.0.1","port":{upstream_port}}},"protocol":{{"type":"socks5"}}}}],
            "outbounds":[],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#
    ))
    .expect("upstream config");
    let upstream_handle =
        spawn_engine(Engine::new(upstream_config).expect("build upstream engine"));
    wait_for_listener(upstream_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"http-in","listen":{{"address":"127.0.0.1","port":{proxy_port}}},"protocol":{{"type":"http"}}}}],
            "outbounds":[{{"tag":"socks-out","protocol":{{"type":"socks5","server":"127.0.0.1","port":{upstream_port}}}}}],
            "route":{{"rules":[],"final":{{"type":"route","outbound":"socks-out"}}}}
        }}"#
    ))
    .expect("outer config");
    let outer_handle = spawn_engine(Engine::new(outer_config).expect("build outer engine"));
    wait_for_listener(proxy_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", proxy_port))
        .await
        .expect("connect proxy");
    client
        .write_all(
            format!("POST http://127.0.0.1:{origin_port}/proxied HTTP/1.1\r\nHost: ignored\r\nProxy-Connection: keep-alive\r\nContent-Length: 2\r\n\r\n{{}}")
                .as_bytes(),
        )
        .await
        .expect("request");
    let (head, body) = read_http_response(&mut client).await;
    assert!(String::from_utf8_lossy(&head)
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked"));
    assert_eq!(body, b"4\r\npong\r\n0\r\n\r\n");
    wait_for("proxied HTTP session", || {
        outer_handle.completed_sessions().iter().any(|session| {
            session.outbound_tag.as_deref() == Some("socks-out")
                && session.outcome.kind() == "chained_relayed"
        })
    })
    .await;

    outer_handle.shutdown().await.expect("shutdown outer");
    upstream_handle.shutdown().await.expect("shutdown upstream");
    origin_task.await.expect("origin task");
}

fn http_config(proxy_port: u16, rules: &str, final_action: &str) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{"tag":"http-in","listen":{{"address":"127.0.0.1","port":{proxy_port}}},"protocol":{{"type":"http"}}}}],
            "outbounds":[],
            "route":{{"rules":{rules},"final":{final_action}}}
        }}"#
    ))
    .expect("parse HTTP config")
}

async fn read_http_response(stream: &mut TcpStream) -> (Vec<u8>, Vec<u8>) {
    let head = read_http_head(stream).await;
    let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
    if text.contains("transfer-encoding: chunked\r\n") {
        let mut body = Vec::new();
        loop {
            let line = read_http_line(stream).await;
            let size = usize::from_str_radix(
                std::str::from_utf8(&line[..line.len() - 2]).expect("chunk size"),
                16,
            )
            .expect("chunk size");
            body.extend_from_slice(&line);
            if size == 0 {
                body.extend_from_slice(&read_http_line(stream).await);
                break;
            }
            let mut chunk = vec![0_u8; size + 2];
            stream.read_exact(&mut chunk).await.expect("chunk");
            body.extend_from_slice(&chunk);
        }
        return (head, body);
    }
    let length = text
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .map(str::trim)
        .map(|value| value.parse::<usize>().expect("content length"))
        .unwrap_or(0);
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).await.expect("response body");
    (head, body)
}

async fn read_http_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    while !line.ends_with(b"\r\n") {
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte).await.expect("line");
        line.push(byte[0]);
    }
    line
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
