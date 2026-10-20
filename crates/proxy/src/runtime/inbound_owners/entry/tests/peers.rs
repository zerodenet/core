use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn actual_peer_io_survives_primary_panic_and_cancelled_cleanup_until_its_real_scope_join() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = proxy.inbound_owners.clone();
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install(
            "peer-owner",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (io, remote) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let mut io = io.unwrap();
    let mut remote = remote.unwrap().0;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let pending = Arc::new(Notify::new());
    let panic = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let pending = pending.clone();
            let panic = panic.clone();
            async move {
                assert!(group.spawn(&host, || async move {
                    let mut bytes = [0];
                    let read = io.read(&mut bytes);
                    tokio::pin!(read);
                    let read = std::future::poll_fn(|cx| {
                        let result = read.as_mut().poll(cx);
                        if result.is_pending() {
                            pending.notify_one();
                        }
                        result
                    })
                    .await?;
                    assert_eq!(read, 0);
                    drop(socket);
                    Ok(())
                })?);
                panic.notified().await;
                panic!("actual primary panic cannot own the only peer JoinSet");
            }
        })
        .unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    panic.notify_one();
    assert!(timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .is_err());
    assert!(UdpSocket::bind(address).await.is_err());
    let original = owner.failure.lock().unwrap().clone().unwrap();
    let mut close =
        Box::pin(registry.shutdown("peer-owner", Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(close);
    assert!(registry.owner("peer-owner").is_some());
    assert!(UdpSocket::bind(address).await.is_err());
    remote.shutdown().await.unwrap();
    assert!(registry
        .shutdown("peer-owner", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        owner.failure.lock().unwrap().as_ref().unwrap(),
        &original
    ));
    assert_eq!(owner.report.lock().unwrap().failed_tasks.len(), 1);
    let _rebound = UdpSocket::bind(address).await.unwrap();
}

#[tokio::test]
async fn successful_peer_joins_reclaim_ids_instead_of_growing_the_persistent_report() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, _) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "peer-history",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    owner
        .spawn_driver(async move {
            for _ in 0..64 {
                let (mut io, mut remote) = tokio::io::duplex(1);
                remote.write_u8(7).await?;
                assert!(group.spawn(&host, || async move {
                    assert_eq!(io.read_u8().await?, 7);
                    Ok(())
                })?);
            }
            group.wait_closed().await
        })
        .unwrap();
    timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .unwrap();
    proxy
        .inbound_owners
        .shutdown("peer-history", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let report = owner.report.lock().unwrap();
    assert_eq!(report.exited_tasks, vec![*owner.driver.get().unwrap()]);
    assert!(report.failed_tasks.is_empty());
}

#[cfg(feature = "raw-ip-runtime")]
#[tokio::test]
async fn explicit_child_groups_share_the_actual_scope_without_borrowing_another_group_quota() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, _) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install("bounded-children", &proxy.protocols, 2, stop)
        .unwrap();
    let first = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let second = installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .unwrap();
    assert!(installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .is_err());
    let owner = installed.owner();
    let (finish, finished) = tokio::sync::oneshot::channel();
    owner
        .spawn_driver(async move {
            finished.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let host = installed.tasks();
    let (release, released) = tokio::sync::oneshot::channel();
    assert!(first
        .spawn_bound(&host, |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            released.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap());
    assert_eq!(first.occupied(), 1);
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rejected = called.clone();
    assert!(!first
        .spawn_bound(&host, |_| {
            rejected.store(true, std::sync::atomic::Ordering::Release);
            async { Ok(()) }
        })
        .unwrap());
    assert!(!called.load(std::sync::atomic::Ordering::Acquire));
    assert!(second
        .spawn_bound(&host, |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap());
    // Body completion and an available peer slot are different facts; only
    // the original scope's consumed TaskExit can reclaim either group record.
    assert_eq!(second.occupied(), 1);
    release.send(()).unwrap();
    finish.send(()).unwrap();
    assert!(installed
        .child_tasks(NonZeroUsize::new(1).unwrap())
        .is_err());
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(first.occupied(), 0);
    assert_eq!(second.occupied(), 0);
    assert_eq!(owner.terminal_for_test().unwrap().joined_tasks(), 3);
}
