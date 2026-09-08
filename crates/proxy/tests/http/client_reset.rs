use std::time::Duration;

use super::*;
use tokio::net::TcpSocket;

#[tokio::test]
async fn client_reset_after_connect_is_not_reported_as_upstream_failure() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let upstream_port = listener.local_addr().unwrap().port();
    let proxy_port = free_port();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let upstream = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0; 81];
        stream.read_exact(&mut header).await.unwrap();
        ready_tx.send(()).unwrap();
        let _ = release_rx.await;
    });
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds": [{"tag":"http-in", "listen":{"address":"127.0.0.1","port":proxy_port}, "protocol":{"type":"http"}}],
        "outbounds": [{"tag":"loopback-ss", "protocol":{"type":"shadowsocks","server":"127.0.0.1","port":upstream_port,"cipher":"chacha20-ietf-poly1305","password":"test-only"}}],
        "route":{"rules":[],"final":{"type":"route","outbound":"loopback-ss"}}
    }).to_string()).unwrap();
    let engine = spawn_engine(Engine::new(config).unwrap());
    wait_for_listener(proxy_port).await;
    let socket = TcpSocket::new_v4().unwrap();
    // Zero linger sends an immediate RST; unlike positive linger it does not
    // wait for queued data on drop. This is the behavior under regression test.
    #[allow(deprecated)]
    socket.set_linger(Some(Duration::ZERO)).unwrap();
    let mut client = socket
        .connect(([127, 0, 0, 1], proxy_port).into())
        .await
        .unwrap();
    client
        .write_all(b"CONNECT chatgpt.com:443 HTTP/1.1\r\nHost: chatgpt.com:443\r\n\r\n")
        .await
        .unwrap();
    let mut response = [0; 39];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"HTTP/1.1 200 Connection Established\r\n\r\n");
    ready_rx.await.unwrap();
    drop(client);
    wait_for("client reset completion", || {
        !engine.completed_sessions().is_empty()
    })
    .await;
    let completed = engine.completed_sessions();
    let flow = completed
        .iter()
        .find(|flow| flow.target == zero_core::Address::Domain("chatgpt.com".to_owned()))
        .unwrap();
    assert_eq!(flow.close_reason.as_deref(), Some("client_error"));
    assert_eq!(flow.failure.as_ref().unwrap().stage, "client_transport");
    assert_eq!(flow.inbound_rx_bytes, 0);
    assert_eq!(flow.outbound_tx_bytes, 81);
    let _ = release_tx.send(());
    upstream.await.unwrap();
    engine.shutdown().await.unwrap();
}
