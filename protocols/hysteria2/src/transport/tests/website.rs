use super::{http3::Masquerade, Hysteria2Website};
use bytes::Bytes;
use std::{io, net::SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_transport::http_server::{HttpExchange, HttpHandler, RequestContext};

#[derive(Default)]
struct Exchange {
    response: Option<http::Response<()>>,
    body: Vec<u8>,
}
#[async_trait::async_trait]
impl HttpExchange for Exchange {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        Ok(None)
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.response = Some(response);
        Ok(())
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.body.extend(data);
        Ok(())
    }
    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn website_redirect_preserves_path_query_and_handles_ipv6_and_old_port() {
    for (host, port, expected) in [
        ("site.example:80", 443, "site.example"),
        ("[::1]:80", 8443, "[::1]:8443"),
    ] {
        let website = Hysteria2Website::new(Masquerade::NotFound, 443, Some(port));
        let request = http::Request::builder()
            .uri("/a%20b?q=1")
            .header("host", host)
            .body(())
            .unwrap();
        let mut exchange = Exchange::default();
        HttpHandler::serve(&website, request, &mut exchange)
            .await
            .unwrap();
        let response = exchange.response.unwrap();
        assert_eq!(response.status(), 301);
        assert_eq!(
            response.headers()["location"],
            format!("https://{expected}/a%20b?q=1")
        );
        assert!(!response.headers().contains_key("alt-svc"));
    }
}
async fn origin<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(mut socket: S) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        request.push(socket.read_u8().await.unwrap());
    }
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nAlt-Svc: origin\r\nConnection: close\r\n\r\norigin").await.unwrap();
    String::from_utf8(request).unwrap()
}
async fn proxy_request(
    url: &str,
    rewrite: bool,
    insecure: bool,
    forwarded: bool,
    tls: bool,
) -> Exchange {
    let policy = Masquerade::proxy_with_options(url, rewrite, insecure, forwarded).unwrap();
    let website = Hysteria2Website::new(policy, 443, None);
    let request = http::Request::builder()
        .uri("/path?q=1")
        .header("host", "site.example:8443")
        .header("x-forwarded-for", "spoofed")
        .header("x-forwarded-host", "spoofed")
        .header("x-forwarded-proto", "spoofed")
        .header("forwarded", "for=spoofed")
        .extension(RequestContext {
            peer: "192.0.2.1:12345".parse::<SocketAddr>().unwrap(),
            tls,
        })
        .body(())
        .unwrap();
    let mut exchange = Exchange::default();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        HttpHandler::serve(&website, request, &mut exchange),
    )
    .await
    .unwrap()
    .unwrap();
    exchange
}
#[tokio::test]
async fn website_proxy_preserves_host_or_rewrites_and_regenerates_forwarded_headers() {
    for forwarded in [false, true] {
        for rewrite in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task =
                tokio::spawn(async move { origin(listener.accept().await.unwrap().0).await });
            let exchange = proxy_request(
                &format!("http://{address}/base?fixed=2"),
                rewrite,
                false,
                forwarded,
                true,
            )
            .await;
            assert_eq!(exchange.body, b"origin");
            assert_eq!(
                exchange.response.unwrap().headers()["alt-svc"],
                "h3=\":443\"; ma=2592000"
            );
            let headers = task.await.unwrap().to_lowercase();
            assert!(
                headers.starts_with("get /base/path?fixed=2&q=1 http/1.1\r\n"),
                "{headers}"
            );
            let host = if rewrite {
                address.to_string()
            } else {
                "site.example:8443".into()
            };
            assert!(
                headers.contains(&format!("\r\nhost: {host}\r\n")),
                "{headers}"
            );
            assert!(!headers.contains("spoofed"));
            assert_eq!(
                headers.contains("x-forwarded-for: 192.0.2.1\r\n"),
                forwarded
            );
            assert_eq!(
                headers.contains("x-forwarded-host: site.example:8443\r\n"),
                forwarded
            );
            assert_eq!(headers.contains("x-forwarded-proto: https\r\n"), forwarded);
        }
    }
}
#[tokio::test]
async fn website_proxy_tls_verifies_by_default_and_insecure_is_explicit() {
    use std::sync::Arc;
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![certificate.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    for insecure in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let acceptor = acceptor.clone();
        let task = tokio::spawn(async move {
            match acceptor.accept(listener.accept().await.unwrap().0).await {
                Ok(stream) => Some(origin(stream).await),
                Err(_) => None,
            }
        });
        let exchange =
            proxy_request(&format!("https://{address}"), false, insecure, false, false).await;
        assert_eq!(
            exchange.response.unwrap().status(),
            if insecure { 200 } else { 502 }
        );
        assert_eq!(task.await.unwrap().is_some(), insecure);
    }
}
#[cfg(unix)]
#[tokio::test]
async fn website_proxy_unix_origin_supports_encoded_socket_path() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("origin socket");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let task = tokio::spawn(async move { origin(listener.accept().await.unwrap().0).await });
    let url = format!("unix://{}", socket.to_str().unwrap().replace(' ', "%20"));
    let exchange = proxy_request(&url, true, false, true, false).await;
    assert_eq!(exchange.body, b"origin");
    let headers = task.await.unwrap().to_lowercase();
    assert!(headers.contains("host: localhost\r\n"), "{headers}");
    assert!(headers.contains("x-forwarded-proto: http\r\n"), "{headers}");
}
