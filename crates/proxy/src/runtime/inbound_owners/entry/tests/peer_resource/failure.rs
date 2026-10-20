use super::*;

#[tokio::test]
async fn real_resource_read_failure_does_not_cancel_another_peers_pending_scope_join() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("failed-close".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "failed-close",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut gate_peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (gate, _) = listener.accept().await.unwrap();
    let mut recovery_peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (recovery, _) = listener.accept().await.unwrap();
    let (address_tx, address_rx) = tokio::sync::oneshot::channel();
    let (mut first, mut finish_first) = tokio::io::duplex(1);
    let (mut second, mut finish_second) = tokio::io::duplex(1);
    let pending = Arc::new(Notify::new());
    let failed = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let group = group.clone();
            let pending = pending.clone();
            let failed = failed.clone();
            async move {
                let for_claim = group.clone();
                assert!(group.spawn_bound(&host, move |binding| {
                    let context = for_claim.resource_context(binding, ingress);
                    async move {
                        context
                            .claim(Instant::now() + Duration::from_secs(2), |_| {
                                Ok((
                                    erase_lifecycle(SocketOwner {
                                        socket: None,
                                        address: Some(address_tx),
                                        gate,
                                        pending: Arc::new(Notify::new()),
                                        gate_done: false,
                                        recovery: Some(recovery),
                                        failed: Some(failed),
                                    }),
                                    (),
                                ))
                            })
                            .await
                            .map_err(std::io::Error::other)?;
                        first.read_u8().await?;
                        Ok(())
                    }
                })?);
                assert!(group.spawn_bound(&host, |_| async move {
                    let read = second.read_u8();
                    tokio::pin!(read);
                    std::future::poll_fn(|cx| {
                        let result = read.as_mut().poll(cx);
                        if result.is_pending() {
                            pending.notify_one();
                        }
                        result
                    })
                    .await?;
                    Ok(())
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                group.wait_closed().await
            }
        })
        .unwrap();
    let address = address_rx.await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    finish_first.write_u8(1).await.unwrap();
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert!(exit.result.is_ok());
    owner.record(exit);
    // The original TCP close gate returns a real UnexpectedEof, while another
    // accepted peer is still suspended in its actual read operation.
    gate_peer.shutdown().await.unwrap();
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    tokio::select! {
        result = &mut closing => panic!("resource failure cancelled sibling join: {result:?}"),
        _ = failed.notified() => {}
    }
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    // Cancellation preserves the same original resource and actual peer scope.
    assert!(UdpSocket::bind(address).await.is_err());
    finish_second.write_u8(1).await.unwrap();
    recovery_peer.write_u8(1).await.unwrap();
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    let _native_closed = UdpSocket::bind(address).await.unwrap();
    assert!(!group.spawn(&host, || async { Ok(()) }).unwrap());
    let failure = owner.failure.lock().unwrap().clone().unwrap();
    let mut source: &(dyn std::error::Error + 'static) = failure.as_ref();
    let mut actual_eof = false;
    loop {
        if let Some(error) = source.downcast_ref::<std::io::Error>() {
            actual_eof |= error.kind() == std::io::ErrorKind::UnexpectedEof;
        }
        let Some(next) = source.source() else {
            break;
        };
        source = next;
    }
    assert!(
        actual_eof,
        "retain the actual original close-gate I/O source"
    );
}
