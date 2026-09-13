#![cfg(feature = "split_http")]
use std::time::Duration;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use zero_transport::{
    profile::OwnedSplitHttpProfile,
    split_http::{accept_xhttp_connection, connect_split_http, SplitHttpRegistry},
};
fn config(mode: &str) -> OwnedSplitHttpProfile {
    OwnedSplitHttpProfile {
        options: Default::default(),
        host: Some("example.com".into()),
        path: "/tunnel?token=1".into(),
        mode: mode.into(),
    }
}
async fn roundtrip(mode: &str) {
    let (post, post_server) = duplex(1024);
    let (get, get_server) = duplex(1024);
    let registry = SplitHttpRegistry::new();
    let _uploads = accept_xhttp_connection(post_server, &config("auto"), &registry);
    let downloads = accept_xhttp_connection(get_server, &config("auto"), &registry);
    let server = tokio::spawn(async move {
        let stream = downloads.accept().await.unwrap();
        let (mut read, mut write) = tokio::io::split(stream);
        let mut buffer = [0; 4096];
        loop {
            let size = read.read(&mut buffer).await.unwrap();
            if size == 0 {
                break;
            }
            write.write_all(&buffer[..size]).await.unwrap();
        }
    });
    let stream = connect_split_http(post, get, &config(mode)).await.unwrap();
    let (mut read, mut write) = tokio::io::split(stream);
    let payload: Vec<_> = (0..300_123).map(|i| (i % 251) as u8).collect();
    let data = payload.clone();
    let writer = tokio::spawn(async move {
        write.write_all(&data).await.unwrap();
        write.flush().await.unwrap();
        write
    });
    let mut output = vec![0; payload.len()];
    tokio::time::timeout(Duration::from_secs(15), read.read_exact(&mut output))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, payload);
    drop(writer.await.unwrap());
    drop(read);
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn packet_up_uses_sequenced_requests_under_backpressure() {
    roundtrip("packet-up").await;
}
#[tokio::test]
async fn stream_up_uses_one_continuous_upload() {
    roundtrip("stream-up").await;
}
#[tokio::test]
async fn auto_without_reality_uses_packet_uploads() {
    roundtrip("auto").await;
}

#[tokio::test]
async fn http2_multiple_downloads_reorder_uploads_and_isolate_duplicate_failure() {
    let (client, server) = duplex(8192);
    let incoming = accept_xhttp_connection(server, &config("auto"), &SplitHttpRegistry::new());
    let (mut client, driver) = h2::client::handshake(client).await.unwrap();
    let driver = tokio::spawn(driver);
    let request = |method: &str, path: &str| {
        http::Request::builder()
            .method(method)
            .header(
                "referer",
                format!("https://example.com/?x_padding={}", "X".repeat(128)),
            )
            .uri(format!("http://example.com{path}"))
            .body(())
            .unwrap()
    };
    let (first, _) = client
        .send_request(request("GET", "/tunnel/first"), true)
        .unwrap();
    let (second, _) = client
        .send_request(request("GET", "/tunnel/second"), true)
        .unwrap();
    let first_response = first.await.unwrap();
    assert_eq!(first_response.status(), 200);
    let second_response = second.await.unwrap();
    assert_eq!(second_response.status(), 200);
    let mut first_stream = incoming.accept().await.unwrap();
    let mut second_stream = incoming.accept().await.unwrap();
    for (path, data) in [
        ("/tunnel/first/1", b"world".as_slice()),
        ("/tunnel/first/0", b"hello"),
        ("/tunnel/second/0", b"other"),
    ] {
        let (response, mut send) = client.send_request(request("POST", path), false).unwrap();
        send.send_data(bytes::Bytes::copy_from_slice(data), true)
            .unwrap();
        assert_eq!(response.await.unwrap().status(), 200);
    }
    let mut output = [0; 10];
    first_stream.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"helloworld");
    let (response, mut send) = client
        .send_request(request("POST", "/tunnel/first/0"), false)
        .unwrap();
    send.send_data(bytes::Bytes::from_static(b"duplicate"), true)
        .unwrap();
    assert_eq!(response.await.unwrap().status(), 409);
    let mut output = [0; 5];
    second_stream.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"other");
    let (response, mut send) = client
        .send_request(request("POST", "/tunnel/second/1"), false)
        .unwrap();
    send.send_data(bytes::Bytes::from_static(b"alive"), true)
        .unwrap();
    assert_eq!(response.await.unwrap().status(), 200);
    second_stream.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"alive");
    driver.abort();
}

#[tokio::test]
async fn route_drop_drains_buffered_download_before_http_eof() {
    let (post, server_post) = duplex(1024);
    let (get, server_get) = duplex(1024);
    let registry = SplitHttpRegistry::new();
    let _upload = accept_xhttp_connection(server_post, &config("auto"), &registry);
    let incoming = accept_xhttp_connection(server_get, &config("auto"), &registry);
    let writer = tokio::spawn(async move {
        let mut stream = incoming.accept().await.unwrap();
        stream.write_all(&vec![7; 1_000_003]).await.unwrap();
        // Keep the physical connection alive while dropping only this route.
        drop(stream);
        tokio::time::sleep(Duration::from_secs(2)).await;
    });
    let mut stream = connect_split_http(post, get, &config("packet-up"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bytes, vec![7; 1_000_003]);
    writer.await.unwrap();
}
