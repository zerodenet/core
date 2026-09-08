use super::{
    http3::Masquerade,
    test_fixtures::{pair, profile},
};
use bytes::{Buf, Bytes};
use std::future::poll_fn;
use tokio::time::{timeout, Duration};
use zero_core::InboundClientResponse;
type Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
async fn request(
    sender: &mut Sender,
    method: &str,
    uri: &str,
    password: Option<&str>,
    body: &[u8],
) -> (http::Response<()>, Vec<u8>) {
    let mut request = http::Request::builder().method(method).uri(uri);
    if let Some(password) = password {
        request = request
            .header("hysteria-auth", password)
            .header("hysteria-cc-rx", "2000000");
    }
    let mut stream = sender
        .send_request(request.body(()).unwrap())
        .await
        .unwrap();
    if !body.is_empty() {
        stream
            .send_data(Bytes::copy_from_slice(body))
            .await
            .unwrap();
    }
    stream.finish().await.unwrap();
    let response = stream.recv_response().await.unwrap();
    let mut result = Vec::new();
    while let Some(mut bytes) = stream.recv_data().await.unwrap() {
        let size = bytes.remaining();
        result.extend_from_slice(&bytes.copy_to_bytes(size));
    }
    (response, result)
}

#[tokio::test]
async fn website_authentication_and_tcp_share_one_http3_connection() {
    timeout(Duration::from_secs(10), async {
        let (client, server) = pair().await;
        let profile =
            profile().with_masquerade(Masquerade::content("hello web", 200, "text/plain").unwrap());
        let server = tokio::spawn(async move {
            let connection = profile
                .accept_authenticated_connection(server)
                .await
                .unwrap();
            let (_, mut stream) = connection.accept_next_tcp_stream().await.unwrap().unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            connection
                .response_protocol()
                .send_ok(&mut stream)
                .await
                .unwrap();
            let mut bytes = [0; 4];
            stream.read_exact(&mut bytes).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.shutdown().await.unwrap();
            connection.datagram_source().closed().await;
        });
        let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(client.clone()))
            .await
            .unwrap();
        let driver = tokio::spawn(async move {
            let _ = poll_fn(|cx| driver.poll_close(cx)).await;
        });
        assert_eq!(
            request(&mut sender, "GET", "https://localhost/", None, b"")
                .await
                .1,
            b"hello web"
        );
        // Wrong credentials receive the same website, and do not poison later auth.
        assert_eq!(
            request(
                &mut sender,
                "POST",
                "https://hysteria/auth",
                Some("wrong"),
                b""
            )
            .await
            .1,
            b"hello web"
        );
        let (response, _) = request(
            &mut sender,
            "POST",
            "https://hysteria/auth",
            Some("test-password"),
            b"",
        )
        .await;
        assert_eq!(response.status().as_u16(), 233);
        assert_eq!(response.headers()["hysteria-cc-rx"], "0");
        assert_eq!(
            request(&mut sender, "GET", "https://localhost/after", None, b"")
                .await
                .1,
            b"hello web"
        );
        // A stalled proxy header must not block other streams on the connection.
        let (mut stalled, _stalled_recv) = client.open_bi().await.unwrap();
        stalled.write_all(&[0x44, 0x01]).await.unwrap();
        let (send, recv) = client.open_bi().await.unwrap();
        let mut stream = super::Hysteria2Stream::new(send, recv);
        let session = zero_core::Session::new(
            1,
            zero_core::Address::Domain("example.com".into()),
            80,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("hysteria2"),
        );
        crate::Hysteria2Outbound
            .establish_tcp_connect(&mut stream, &session)
            .await
            .unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream.write_all(b"ping").await.unwrap();
        let mut bytes = [0; 4];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"ping");
        client.close(0u32.into(), b"");
        server.await.unwrap();
        driver.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn masquerade_reverse_proxy_forwards_method_path_and_body() {
    timeout(Duration::from_secs(10), async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = origin.local_addr().unwrap();
        let origin = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop { let mut buf = [0u8; 4096]; let size = socket.read(&mut buf).await.unwrap(); assert_ne!(size, 0); bytes.extend_from_slice(&buf[..size]); if bytes.ends_with(b"payload") { break; } }
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.starts_with("POST /base/path?q=1 HTTP/1.1\r\n"), "{text}");
            assert!(text.to_ascii_lowercase().contains(&format!("host: {address}")));
            socket.write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 6\r\nConnection: close\r\nX-Origin: yes\r\n\r\norigin").await.unwrap();
        });
        let (client, server) = pair().await;
        let profile = profile().with_masquerade(Masquerade::proxy(&format!("http://{address}/base"), true).unwrap());
        let server = tokio::spawn(async move { let _ = profile.accept_authenticated_connection(server).await; });
        let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(client.clone())).await.unwrap();
        let driver = tokio::spawn(async move { let _ = poll_fn(|cx| driver.poll_close(cx)).await; });
        let (response, body) = request(&mut sender, "POST", "https://website/path?q=1", None, b"payload").await;
        assert_eq!(response.status(), 201); assert_eq!(body, b"origin");
        assert_eq!(response.headers()["x-origin"], "yes"); assert!(!response.headers().contains_key("connection"));
        client.close(0u32.into(), b""); origin.await.unwrap(); server.await.unwrap(); driver.abort();
    }).await.unwrap();
}

#[tokio::test]
async fn masquerade_static_site_serves_index_and_denies_traversal() {
    timeout(Duration::from_secs(10), async {
        let path = std::env::temp_dir().join(format!(
            "zero-hy2-site-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("index.html"), "site index").unwrap();
        let (client, server) = pair().await;
        let profile =
            profile().with_masquerade(Masquerade::file(path.to_str().unwrap(), None).unwrap());
        let server = tokio::spawn(async move {
            let _ = profile.accept_authenticated_connection(server).await;
        });
        let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(client.clone()))
            .await
            .unwrap();
        let driver = tokio::spawn(async move {
            let _ = poll_fn(|cx| driver.poll_close(cx)).await;
        });
        let (response, body) = request(&mut sender, "GET", "https://website/", None, b"").await;
        assert_eq!(response.status(), 200);
        assert_eq!(body, b"site index");
        assert_eq!(
            request(
                &mut sender,
                "GET",
                "https://website/%2e%2e/secret",
                None,
                b""
            )
            .await
            .0
            .status(),
            404
        );
        let (response, body) = request(&mut sender, "HEAD", "https://website/", None, b"").await;
        assert_eq!(response.headers()["content-length"], "10");
        assert!(body.is_empty());
        client.close(0u32.into(), b"");
        server.await.unwrap();
        driver.abort();
        std::fs::remove_dir_all(path).unwrap();
    })
    .await
    .unwrap();
}
