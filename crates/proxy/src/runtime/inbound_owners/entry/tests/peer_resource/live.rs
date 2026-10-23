//! Actual native cleanup runs while the same listener keeps accepting children.
use super::*;

#[tokio::test]
async fn inline_observer_consumes_other_exits_during_pending_close_then_reuses_only_closed_slot() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("live-cleanup".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install("live-cleanup", &proxy.protocols, 2, stop)
        .unwrap();
    let first = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let second = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let host = installed.tasks();
    let owner = installed.owner();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut release_close = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (gate, _) = listener.accept().await.unwrap();
    let (address_tx, address_rx) = tokio::sync::oneshot::channel();
    let (second_tx, second_rx) = tokio::sync::oneshot::channel();
    let (mut first_io, mut release_first) = tokio::io::duplex(1);
    let (mut second_io, mut release_second) = tokio::io::duplex(1);
    let pending = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let first = first.clone();
            let second = second.clone();
            let host = host.clone();
            let pending = pending.clone();
            async move {
                let claim_group = first.clone();
                assert!(first.spawn_bound(&host, |binding| {
                    let context = claim_group.resource_context(binding, ingress);
                    async move {
                        context
                            .claim(Instant::now() + Duration::from_secs(2), |_| {
                                Ok((
                                    erase_lifecycle(SocketOwner {
                                        socket: None,
                                        address: Some(address_tx),
                                        gate,
                                        pending,
                                        gate_done: false,
                                        recovery: None,
                                        failed: None,
                                    }),
                                    (),
                                ))
                            })
                            .await
                            .map_err(std::io::Error::other)?;
                        first_io.read_u8().await?;
                        Ok(())
                    }
                })?);
                assert!(second.spawn_bound(&host, |binding| async move {
                    binding.wait_bound().await.map_err(std::io::Error::other)?;
                    second_tx.send(binding).unwrap();
                    second_io.read_u8().await?;
                    Ok(())
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                first.wait_closed().await?;
                second.wait_closed().await
            }
        })
        .unwrap();
    let original = address_rx.await.unwrap();
    let second_binding = second_rx.await.unwrap();
    let second_task = second_binding.wait_bound().await.unwrap();
    assert!(UdpSocket::bind(original).await.is_err());
    release_first.write_u8(1).await.unwrap();
    let mut observation = Box::pin(owner.observe_driver());
    tokio::select! {
        result = &mut observation => panic!("listener must remain live while actual close is pending: {result:?}"),
        _ = pending.notified() => {},
    }
    assert_eq!(
        first.occupied(),
        1,
        "TaskExit alone does not release the native slot"
    );
    assert!(UdpSocket::bind(original).await.is_err());
    release_second.write_u8(1).await.unwrap();
    let joined = tokio::select! {
        result = &mut observation => panic!("actual other join must not finish the listener: {result:?}"),
        result = second_binding.wait_joined(Instant::now() + Duration::from_secs(2)) => result.unwrap(),
    };
    assert_eq!(joined.task(), second_task);
    assert_eq!(
        second.occupied(),
        0,
        "original Scope consumed the independent child's exit during pending native cleanup"
    );
    drop(observation);
    assert_eq!(first.occupied(), 1);
    assert!(
        UdpSocket::bind(original).await.is_err(),
        "cancelled observer retains its actual owner"
    );
    release_close.write_u8(1).await.unwrap();
    let mut resumed = Box::pin(owner.observe_driver());
    while first.occupied() != 0 {
        tokio::select! {
            result = &mut resumed => panic!("cleanup must not stop the live listener: {result:?}"),
            _ = first.changed() => {},
        }
    }
    let _held_probe = UdpSocket::bind(original).await.unwrap();
    let (active, activated) = tokio::sync::oneshot::channel();
    assert!(
        first
            .spawn_bound(&host, |binding| async move {
                binding.wait_bound().await.map_err(std::io::Error::other)?;
                active.send(()).unwrap();
                Ok(())
            })
            .unwrap(),
        "same healthy group can now accept a new actual child"
    );
    activated.await.unwrap();
    owner.stop();
    resumed.await.unwrap();
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(owner.terminal_for_test().unwrap().joined_tasks(), 4);
}
