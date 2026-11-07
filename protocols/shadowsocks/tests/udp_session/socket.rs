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
    let socket_lifetime = Arc::downgrade(&socket);
    let codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> = Arc::new(
        ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, b"secret"),
    );
    let (recv_tx, mut received) = broadcast::channel(32);
    let responses = Arc::new(Responses::default());
    let receiver_task = tokio::spawn(recv_loop(
        Socket::Library(socket.clone()),
        endpoint,
        codec.clone(),
        recv_tx.clone(),
        None,
        responses.clone(),
    ));
    let receiver = receiver_task.abort_handle();
    let flow = ShadowsocksUdpSocketFlow {
        plugin: None,
        socket: Socket::Library(socket),
        endpoint,
        codec: codec.clone(),
        recv_tx: recv_tx.downgrade(),
        responses: responses.clone(),
        _receiver: Some(Arc::new(ReceiverGuard(receiver.clone(), responses))),
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
    let cancelled = tokio::time::timeout(std::time::Duration::from_secs(2), receiver_task)
        .await
        .expect("idle receiver cancellation must finish")
        .expect_err("dropping the owner must cancel its receiver");
    assert!(cancelled.is_cancelled());
    assert!(receiver.is_finished());
    // Keep the subscription alive: cancellation is tied to the flow owner.
    // Observe the owned socket directly. Another parallel test can claim the
    // released ephemeral port before a rebind, even when cleanup succeeded.
    assert!(socket_lifetime.upgrade().is_none());
}

#[tokio::test]
async fn actual_receive_future_routes_to_the_same_cache_without_a_forwarding_task() {
    let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let endpoint = server.local_addr().unwrap();
    let socket = Arc::new(
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(endpoint, None)
            .await
            .unwrap(),
    );
    let local = SocketAddr::new(endpoint.ip(), socket.local_addr().unwrap().port());
    let socket_lifetime = Arc::downgrade(&socket);
    let codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> = Arc::new(
        ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, b"same-secret"),
    );
    let (recv_tx, _) = broadcast::channel(32);
    let responses = Arc::new(Responses::default());
    let receiver_task = tokio::spawn(recv_loop(
        Socket::Library(socket.clone()),
        endpoint,
        codec.clone(),
        recv_tx.clone(),
        None,
        responses.clone(),
    ));
    let receiver = receiver_task.abort_handle();
    let flow = ShadowsocksUdpSocketFlow {
        plugin: None,
        socket: Socket::Library(socket),
        endpoint,
        codec: codec.clone(),
        recv_tx: recv_tx.downgrade(),
        responses: responses.clone(),
        _receiver: Some(Arc::new(ReceiverGuard(receiver.clone(), responses))),
    };
    let (send, mut received) = tokio::sync::mpsc::channel(2);
    let sink: Arc<ShadowsocksUdpResponseSink> = Arc::new(move |response| {
        send.try_send(response).unwrap();
    });
    assert!(flow.install_response_sink(&sink));
    let other_sink: Arc<ShadowsocksUdpResponseSink> =
        Arc::new(|_| panic!("foreign cache stole FIFO"));
    assert!(!flow.install_response_sink(&other_sink));
    for payload in [vec![], vec![9; 8_000]] {
        let datagram = codec
            .encode(&Address::Ipv4([1, 2, 3, 4]), 53, &payload)
            .unwrap();
        server.send_to(&datagram, local).await.unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response, (Address::Ipv4([1, 2, 3, 4]), 53, payload));
    }
    drop(flow);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), receiver_task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert!(receiver.is_finished());
    assert!(socket_lifetime.upgrade().is_none());
}

#[tokio::test]
async fn real_shared_pending_budget_counts_received_unclaimed_responses_and_recovers_without_new_socket(
) {
    use zero_platform_tokio::TokioDatagramSocket;
    let total = Arc::new(responses::budget::PendingBudget::default());
    responses::budget::PendingProposal::new(
        &total,
        ShadowsocksUdpPendingLimits {
            per_cache: 1,
            total: 2,
        },
    )
    .unwrap()
    .activate()
    .unwrap();
    let mut servers = Vec::new();
    let mut flows = Vec::new();
    let mut drivers = Vec::new();
    for _ in 0..3 {
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = server.local_addr().unwrap();
        let socket = Arc::new(
            TokioDatagramSocket::bind_for_peer_on(endpoint, None)
                .await
                .unwrap(),
        );
        let codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>> = Arc::new(
            ShadowsocksDatagramCodec::new(CipherKind::Aes128Gcm, b"budget-original"),
        );
        let responses = Arc::new(Responses::with_budget(1, total.clone()));
        let (send, _) = broadcast::channel(32);
        let driver = tokio::spawn(recv_loop(
            Socket::Library(socket.clone()),
            endpoint,
            codec.clone(),
            send.clone(),
            None,
            responses.clone(),
        ));
        flows.push(ShadowsocksUdpSocketFlow {
            plugin: None,
            socket: Socket::Library(socket),
            endpoint,
            codec,
            recv_tx: send.downgrade(),
            responses: responses.clone(),
            _receiver: Some(Arc::new(ReceiverGuard(driver.abort_handle(), responses))),
        });
        servers.push(server);
        drivers.push(driver);
    }
    let target = Address::Ipv4([192, 0, 2, 7]);
    let mut first = flows[0]
        .request_datagram(&target, 53, b"first")
        .await
        .unwrap();
    let second = flows[1]
        .request_datagram(&target, 53, b"second")
        .await
        .unwrap();
    assert!(flows[0]
        .request_datagram(&target, 53, b"denied-local")
        .await
        .is_err());
    assert!(responses::budget::PendingProposal::new(
        &total,
        ShadowsocksUdpPendingLimits {
            per_cache: 1,
            total: 1
        }
    )
    .unwrap()
    .activate()
    .is_err());
    let mut bytes = [0; 65535];
    let (size, peer) = servers[0].recv_from(&mut bytes).await.unwrap();
    assert_eq!(flows[0].codec.decode(&bytes[..size]).unwrap().2, b"first");
    let response = flows[0].codec.encode(&target, 53, b"response").unwrap();
    servers[0].send_to(&response, peer).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !first.received_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // No request is submitted merely because its prior response might already
    // be in the actual receiver. The original future still owns its quota.
    assert!(flows[0]
        .request_datagram(&target, 53, b"denied-unclaimed")
        .await
        .is_err());
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), &mut first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.2, b"response");
    // Keep the completed future object alive: actual handoff, rather than its
    // eventual destructor, releases the response-only credit.
    let third = flows[0]
        .request_datagram(&target, 53, b"third")
        .await
        .unwrap();
    let (size, same_peer) = servers[0].recv_from(&mut bytes).await.unwrap();
    assert_eq!(same_peer, peer);
    assert_eq!(flows[0].codec.decode(&bytes[..size]).unwrap().2, b"third");
    assert!(flows[2]
        .request_datagram(&target, 54, b"denied-total")
        .await
        .is_err());
    drop(second);
    drop(third);
    let last = flows[2]
        .request_datagram(&target, 54, b"restored")
        .await
        .unwrap();
    let (size, _) = servers[2].recv_from(&mut bytes).await.unwrap();
    assert_eq!(
        flows[2].codec.decode(&bytes[..size]).unwrap().2,
        b"restored"
    );
    drop(last);
    drop(first);
    drop(flows);
    for driver in drivers {
        assert!(driver.await.unwrap_err().is_cancelled());
    }
}
