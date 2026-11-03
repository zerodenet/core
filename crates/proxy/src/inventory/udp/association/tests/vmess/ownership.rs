use super::*;
#[tokio::test]
async fn ordinary_vmess_datagram_retains_original_control_until_actual_peer_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(
        listener.local_addr().unwrap().port(),
        serde_json::json!({"final":{"type":"route","outbound":"node"}}),
        "aes-128-gcm",
    )
    .await;
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
            ID.into(),
            "aes-128-gcm".into(),
            None::<String>,
            None,
            None,
            None,
            None,
            None,
        )])
        .unwrap();
        let (_, mut stream) = profile
            .accept_request_stream(
                vmess::inbound::VmessInbound,
                zero_platform_tokio::TokioSocket::new(stream),
            )
            .await
            .unwrap();
        let len = stream.read_u16().await.unwrap();
        let mut body = vec![0; len as usize];
        stream.read_exact(&mut body).await.unwrap();
        stream.write_u16(len).await.unwrap();
        stream.write_all(&body).await.unwrap();
        stream.flush().await.unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
        eof_tx.send(()).unwrap();
        release_rx.await.unwrap();
        stream.shutdown().await.unwrap();
    });
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
    dispatch.dispatch(input(53, b"probe")).await.unwrap();
    let (_, tasks, _) = dispatch.poll_sockets();
    timeout(Duration::from_secs(3), tasks.join_next_with_id())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
    let _ = dispatch.finish_all();
    let incomplete = retired
        .wait_closed(Instant::now() + Duration::from_millis(30))
        .await
        .unwrap();
    assert!(
        !incomplete.is_complete(),
        "cancellation and dropping data views cannot close the original"
    );
    timeout(Duration::from_secs(3), eof_rx)
        .await
        .unwrap()
        .unwrap();
    release_tx.send(()).unwrap();
    let closed = retired
        .wait_closed(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(closed.is_complete());
    assert!(!closed.closed_io.is_empty());
    server.await.unwrap();
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}
