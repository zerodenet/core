//! Actual unauthenticated H3 appearance, then authenticated TCP/UDP on the same
//! production listener. The origin's real EOF is wire evidence, not close proof.
use super::*;
use bytes::Buf;
pub(super) struct Fixture {
    origin: Option<TcpListener>,
    kind: String,
    tls: Option<tokio_rustls::TlsAcceptor>,
}
impl Fixture {
    pub(super) async fn new(
        carrier: &str,
        root: &std::path::Path,
        protocol: &mut serde_json::Value,
        certificate: &rcgen::CertifiedKey<rcgen::KeyPair>,
    ) -> Self {
        let mut origin = None;
        let mut tls = None;
        if carrier == "hysteria-file" {
            let root = &root.join("site");
            std::fs::create_dir(root).unwrap();
            std::fs::write(root.join("asset.txt"), b"prefix-payload-tail").unwrap();
            std::fs::create_dir(root.join("listing")).unwrap();
            std::fs::write(root.join("listing/a&b.txt"), b"entry").unwrap();
            #[cfg(unix)]
            std::os::unix::fs::symlink(root.parent().unwrap().join("key.pem"), root.join("escape"))
                .unwrap();
            protocol["hysteria"]["masquerade"] =
                serde_json::json!({"type":"file","directory":root});
        } else if carrier.starts_with("hysteria-proxy") {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let secure = carrier.ends_with("https") || carrier.ends_with("h2");
            let scheme = if secure { "https" } else { "http" };
            protocol["hysteria"]["masquerade"] = serde_json::json!({"type":"proxy","url":format!("{scheme}://{address}/origin?a=2"),"rewrite_host":false,"insecure":secure});
            if secure {
                let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        certificate.signing_key.serialize_der(),
                    )
                    .into(),
                )
                .unwrap();
                config.alpn_protocols = vec![if carrier.ends_with("h2") {
                    b"h2".to_vec()
                } else {
                    b"http/1.1".to_vec()
                }];
                tls = Some(tokio_rustls::TlsAcceptor::from(Arc::new(config)));
            }
            origin = Some(listener);
        }
        Self {
            origin,
            kind: carrier.into(),
            tls,
        }
    }
    pub(super) async fn exercise(&self, client: &Client, address: std::net::SocketAddr) {
        let Client::Quic(endpoint) = client else {
            return;
        };
        if self.kind == "hysteria-file" {
            let (response, body) = web(
                endpoint,
                address,
                "GET",
                "/asset.txt",
                &[("range", "bytes=7-13")],
                b"",
            )
            .await;
            assert_eq!(response.status(), 206);
            assert_eq!(body, b"payload");
            assert_eq!(response.headers()["content-range"], "bytes 7-13/19");
            let (head, body) = web(endpoint, address, "HEAD", "/asset.txt", &[], b"").await;
            assert_eq!(head.status(), 200);
            assert!(body.is_empty());
            assert_eq!(head.headers()["content-length"], "19");
            let modified = head.headers()["last-modified"].to_str().unwrap();
            let (conditional, body) = web(
                endpoint,
                address,
                "GET",
                "/asset.txt",
                &[("if-modified-since", modified)],
                b"",
            )
            .await;
            assert_eq!(conditional.status(), 304);
            assert!(body.is_empty());
            let (listing, body) = web(endpoint, address, "GET", "/listing/", &[], b"").await;
            assert_eq!(listing.status(), 200);
            assert!(String::from_utf8(body).unwrap().contains("a&amp;b.txt"));
            let (redirect, _) = web(endpoint, address, "GET", "/listing", &[], b"").await;
            assert_eq!(redirect.status(), 301);
            assert_eq!(redirect.headers()["location"], "listing/");
            #[cfg(unix)]
            {
                let (denied, _) = web(endpoint, address, "GET", "/escape", &[], b"").await;
                assert_eq!(
                    denied.status(),
                    404,
                    "same canonical-root HTTP behavior, never serve TLS material"
                );
            }
        } else if let Some(origin) = &self.origin {
            let origin_waiting = tokio::sync::Notify::new();
            let backend = async {
                let (io, _) = origin.accept().await.unwrap();
                let mut io: zero_platform_tokio::contract::stream::BoxStream =
                    if let Some(tls) = &self.tls {
                        Box::pin(tls.accept(io).await.unwrap())
                    } else {
                        Box::pin(io)
                    };
                if self.kind.ends_with("h2") {
                    return h2_origin(io).await;
                }
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    io.read_exact(&mut byte).await.unwrap();
                    headers.push(byte[0]);
                    assert!(headers.len() < 65536);
                }
                let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
                assert!(
                    headers.starts_with("post /origin/item%2fsub?a=2&q=1 http/1.1\r\n"),
                    "{headers}"
                );
                assert!(headers.contains("\r\nhost: front.example\r\n"), "{headers}");
                assert!(!headers.contains("x-forwarded-for"));
                let mut body = [0; 6];
                io.read_exact(&mut body).await.unwrap();
                assert_eq!(&body, b"upload");
                let response: &[u8] = if self.kind.ends_with("cancel") {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nX-Origin: real\r\n\r\n"
                } else {
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nX-Origin: real\r\n\r\ndownload"
                };
                io.write_all(response).await.unwrap();
                if self.kind.ends_with("cancel") {
                    let mut byte = [0];
                    let mut pending = Box::pin(io.read(&mut byte));
                    std::future::poll_fn(|cx| {
                        assert!(pending.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    origin_waiting.notify_one();
                    assert_eq!(
                        pending.await.unwrap(),
                        0,
                        "real pending origin I/O ends on cancellation"
                    );
                    return;
                }
                let closed = io.read(&mut [0]).await;
                assert!(
                    matches!(closed, Ok(0))
                        || (self.tls.is_some()
                            && matches!(closed,Err(ref error) if error.kind()==std::io::ErrorKind::UnexpectedEof)),
                    "origin stream did not retire: {closed:?}"
                );
            };
            let request = async {
                let (response, body) = web_using(
                    endpoint,
                    address,
                    "POST",
                    "/item%2Fsub?q=1",
                    &[("x-forwarded-for", "untrusted")],
                    b"upload",
                    self.kind.ends_with("cancel"),
                    Some(&origin_waiting),
                )
                .await;
                assert_eq!(response.status(), 200);
                assert_eq!(response.headers()["x-origin"], "real");
                if !self.kind.ends_with("cancel") {
                    assert_eq!(body, b"download");
                }
            };
            tokio::join!(backend, request);
        }
    }
}
async fn web(
    endpoint: &quinn::Endpoint,
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (http_uri::Response<()>, Vec<u8>) {
    web_using(endpoint, address, method, path, headers, body, false, None).await
}
async fn web_using(
    endpoint: &quinn::Endpoint,
    address: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    cancel: bool,
    origin_waiting: Option<&tokio::sync::Notify>,
) -> (http_uri::Response<()>, Vec<u8>) {
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut driver, mut requests) = h3::client::new(h3_quinn::Connection::new(connection.clone()))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        std::future::poll_fn(|cx| driver.poll_close(cx)).await;
    });
    let mut request = http_uri::Request::builder()
        .method(method)
        .uri(format!("https://front.example{path}"));
    for (key, value) in headers {
        request = request.header(*key, *value);
    }
    if !body.is_empty() {
        request = request.header("content-length", body.len());
    }
    let mut stream = requests
        .send_request(request.body(()).unwrap())
        .await
        .unwrap();
    if !body.is_empty() {
        stream
            .send_data(bytes::Bytes::copy_from_slice(body))
            .await
            .unwrap();
    }
    stream.finish().await.unwrap();
    let response = stream.recv_response().await.unwrap();
    let mut bytes = Vec::new();
    if cancel {
        let mut waiting = Box::pin(stream.recv_data());
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        origin_waiting.unwrap().notified().await;
    } else {
        while let Some(mut chunk) = stream.recv_data().await.unwrap() {
            let n = chunk.remaining();
            bytes.extend_from_slice(&chunk.copy_to_bytes(n));
        }
    }
    drop(stream);
    drop(requests);
    connection.close(0u32.into(), b"appearance finished");
    driver.await.unwrap();
    (response, bytes)
}

async fn h2_origin(io: zero_platform_tokio::contract::stream::BoxStream) {
    let mut connection = h2::server::handshake(io).await.unwrap();
    let (request, mut reply) = connection.accept().await.unwrap().unwrap();
    assert_eq!(
        request.uri().path_and_query().unwrap().as_str(),
        "/origin/item%2Fsub?a=2&q=1"
    );
    assert!(
        request
            .headers()
            .get("host")
            .is_some_and(|h| h == "front.example")
            || request.uri().host() == Some("front.example")
    );
    assert!(!request.headers().contains_key("x-forwarded-for"));
    let exchange = async move {
        let mut body = request.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.unwrap();
            body.flow_control().release_capacity(chunk.len()).unwrap();
            bytes.extend_from_slice(&chunk);
        }
        assert_eq!(bytes, b"upload");
        let mut send = reply
            .send_response(
                http_uri::Response::builder()
                    .header("content-length", "8")
                    .header("x-origin", "real")
                    .body(())
                    .unwrap(),
                false,
            )
            .unwrap();
        send.send_data(bytes::Bytes::from_static(b"download"), true)
            .unwrap();
    };
    let drive = async {
        while let Some(Ok(_)) = connection.accept().await {
            panic!("no second origin request or hidden pool");
        }
    };
    tokio::join!(exchange, drive);
}
