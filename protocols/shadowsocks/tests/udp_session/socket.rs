use super::*;
use crate::shared::CipherKind;
use crate::udp::ShadowsocksDatagramCodec;

#[tokio::test]
async fn large_response_is_complete_and_drop_releases_idle_socket() {
    let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let endpoint = server.local_addr().unwrap();
    let socket = Arc::new(
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(endpoint, None)
            .await
            .unwrap(),
    );
    let local = SocketAddr::new(endpoint.ip(), socket.local_addr().unwrap().port());
    let codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> = Arc::new(
        ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, b"secret"),
    );
    let (recv_tx, mut received) = broadcast::channel(32);
    let receiver = tokio::spawn(recv_loop(
        socket.clone(),
        endpoint,
        codec.clone(),
        recv_tx.clone(),
        None,
    ))
    .abort_handle();
    let flow = ShadowsocksUdpSocketFlow {
        plugin: None,
        socket,
        endpoint,
        codec: codec.clone(),
        recv_tx: recv_tx.downgrade(),
        receiver: receiver.clone(),
    };
    let payload = vec![7; 8_000];
    let datagram = codec
        .encode(&Address::Ipv4([1, 2, 3, 4]), 53, &payload)
        .unwrap();
    // An authenticated datagram from another endpoint must not enter this flow.
    let other = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    other.send_to(&datagram, local).await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), received.recv())
            .await
            .is_err()
    );
    server.send_to(&datagram, local).await.unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.2, payload);
    drop(flow);
    tokio::task::yield_now().await;
    assert!(receiver.is_finished());
    // Keep the subscription alive: cancellation is tied to the flow owner.
    let rebound = tokio::net::UdpSocket::bind(local).await.unwrap();
    drop(rebound);
}
