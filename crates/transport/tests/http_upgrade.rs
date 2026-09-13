#![cfg(feature = "http_upgrade")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Profile;
async fn read_request_headers(stream: &mut tokio::io::DuplexStream) {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(
            bytes.len() < 4096,
            "upgrade request headers exceeded fixture limit"
        );
        bytes.push(stream.read_u8().await.unwrap());
    }
    assert!(bytes.starts_with(b"GET /upgrade HTTP/1.1\r\n"));
}
impl zero_traits::HttpUpgradeTransportProfile for Profile {
    fn path(&self) -> &str {
        "/upgrade"
    }
    fn host(&self) -> Option<&str> {
        Some("localhost")
    }
}
#[tokio::test]
async fn preserves_binary_data_coalesced_with_upgrade_request() {
    let (mut client, server) = tokio::io::duplex(4096);
    let task = tokio::spawn(async move {
        let mut stream = zero_transport::http_upgrade::accept_http_upgrade(server, &Profile)
            .await
            .unwrap();
        let mut data = [0; 3];
        stream.read_exact(&mut data).await.unwrap();
        assert_eq!(data, [255, 0, 128]);
    });
    client.write_all(b"GET /upgrade HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n\xff\x00\x80").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn preserves_binary_data_coalesced_with_upgrade_response() {
    let (client, mut server) = tokio::io::duplex(4096);
    let task = tokio::spawn(async move {
        read_request_headers(&mut server).await;
        server.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n\xff\x00\x80").await.unwrap();
    });
    let mut stream = zero_transport::http_upgrade::connect_http_upgrade(client, &Profile)
        .await
        .unwrap();
    let mut data = [0; 3];
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read_exact(&mut data),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(data, [255, 0, 128]);
    task.await.unwrap();
}

struct Early;
impl zero_traits::HttpUpgradeTransportProfile for Early {
    fn path(&self) -> &str {
        "/upgrade?ed=2048"
    }
    fn host(&self) -> Option<&str> {
        Some("localhost")
    }
}
#[tokio::test]
async fn early_upload_precedes_response_and_response_prefix_survives_cancel() {
    let (client, mut server) = tokio::io::duplex(4096);
    let mut client = zero_transport::http_upgrade::connect_http_upgrade(client, &Early)
        .await
        .unwrap();
    client.write_all(b"early").await.unwrap();
    client.flush().await.unwrap();
    let mut bytes = [0; 4096];
    let n = server.read(&mut bytes).await.unwrap();
    let request = &bytes[..n];
    assert!(request.starts_with(b"GET /upgrade HTTP/1.1"));
    assert!(request.ends_with(b"early"));
    server
        .write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnec")
        .await
        .unwrap();
    let mut response = [0; 3];
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(10),
        client.read_exact(&mut response)
    )
    .await
    .is_err());
    server
        .write_all(b"tion: Upgrade\r\nUpgrade: websocket\r\n\r\n\xff\x00\x80")
        .await
        .unwrap();
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [255, 0, 128]);
}
#[tokio::test]
async fn rejects_success_code_without_upgrade_contract() {
    let (client, mut server) = tokio::io::duplex(4096);
    let task = tokio::spawn(async move {
        read_request_headers(&mut server).await;
        server
            .write_all(b"HTTP/1.1 101 Switching Protocols\r\n\r\n")
            .await
            .unwrap();
    });
    assert!(
        zero_transport::http_upgrade::connect_http_upgrade(client, &Profile)
            .await
            .is_err()
    );
    task.await.unwrap();
}
