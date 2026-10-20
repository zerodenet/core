use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

#[tokio::test]
async fn real_fault_receipt_publishes_before_the_actual_supervisor_consumes_it() {
    prove_recovery(false).await;
}
#[tokio::test]
async fn actual_supervisor_consumption_before_publication_preserves_the_original_fault() {
    prove_recovery(true).await;
}
async fn prove_recovery(consume_before_publication: bool) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = proxy.inbound_owners.clone();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("recovery".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let (stop, mut stopping) = watch::channel(false);
    let installed = registry
        .install(
            "recovery",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let weak_owner = Arc::downgrade(&owner);
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut gate_peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (gate, _) = listener.accept().await.unwrap();
    let (address_tx, address_rx) = tokio::sync::oneshot::channel();
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (mut io, mut panic_peer) = tokio::io::duplex(1);
    let pending = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let group = group.clone();
            let host = host.clone();
            let pending = pending.clone();
            async move {
                let for_claim = group.clone();
                assert!(group.spawn_bound(&host, move |binding| {
                    binding_tx.send(binding.clone()).unwrap();
                    let context = for_claim.resource_context(binding, ingress);
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
                        io.read_u8().await?;
                        panic!("actual peer failure retained across physical recovery");
                    }
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                group.wait_closed().await
            }
        })
        .unwrap();
    let address = address_rx.await.unwrap();
    let binding = binding_rx.await.unwrap();
    let peer_task = binding.wait_bound().await.unwrap();
    panic_peer.write_u8(1).await.unwrap();
    let observer = {
        let owner = owner.clone();
        tokio::spawn(async move {
            owner
                .observe_driver()
                .await
                .map_err(|source| owner.observer_error(source))
        })
    };
    let mut confirming =
        Box::pin(registry.confirm_shutdown("recovery", Instant::now() + Duration::from_secs(2)));
    tokio::select! {
        result = &mut confirming => panic!("native close gate must remain pending: {result:?}"),
        _ = pending.notified() => {}
    }
    drop(confirming);
    assert!(UdpSocket::bind(address).await.is_err());
    let (new_stop, _) = watch::channel(false);
    assert!(registry
        .install("recovery", &proxy.protocols, 0, new_stop)
        .is_err());
    gate_peer.write_u8(1).await.unwrap();
    let observed = timeout(Duration::from_secs(2), observer)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    if consume_before_publication {
        assert!(!crate::runtime::inbound_owners::consume_reported_error(
            &observed
        ));
    }
    let first = registry
        .confirm_shutdown("recovery", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let replay = registry
        .confirm_shutdown("recovery", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert!(std::ptr::eq(first.terminal(), replay.terminal()));
    assert!(first.terminal().joined_tasks() >= 2);
    assert_eq!(first.terminal().closed_io_receipts(), 1);
    let old_instance = first.terminal().identity();
    let original = first.terminal().failure().unwrap();
    let actual = first
        .terminal()
        .task_failures()
        .iter()
        .find(|failure| failure.id == peer_task)
        .unwrap();
    assert_eq!(actual.class, TaskClass::Critical);
    assert!(Arc::ptr_eq(&actual.source, &original));
    crate::runtime::inbound_owners::test_support::assert_tracked_panic(
        &binding,
        peer_task,
        actual.class,
        actual.source.as_ref(),
    );
    let _physical_confirmation = UdpSocket::bind(address).await.unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    drop(receiver);
    assert!(registry
        .release_closed(first, |terminal| {
            sender
                .send(terminal.failure().unwrap())
                .map_err(std::io::Error::other)?;
            Ok(())
        })
        .is_err());
    assert!(registry.owner("recovery").is_some());
    let panicking = registry
        .confirm_shutdown("recovery", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        registry.release_closed::<()>(panicking, |_| {
            panic!("actual publication attempt interrupted")
        })
    }));
    assert!(panic.is_err());
    assert!(registry.owner("recovery").is_some());
    let retry = registry
        .confirm_shutdown("recovery", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert!(std::ptr::eq(replay.terminal(), retry.terminal()));
    let published = AtomicUsize::new(0);
    let handed_off = registry
        .release_closed(retry, |terminal| {
            published.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(terminal.failure().unwrap())
        })
        .unwrap();
    assert!(Arc::ptr_eq(&original, &handed_off));
    assert!(crate::runtime::inbound_owners::consume_reported_error(
        &observed
    ));
    assert!(crate::runtime::inbound_owners::consume_reported_error(
        &observed
    ));
    assert!(!crate::runtime::inbound_owners::consume_reported_error(
        &EngineError::NoInbounds
    ));
    assert!(registry.owner("recovery").is_none());
    let (new_stop, _) = watch::channel(false);
    let replacement = registry
        .install("recovery", &proxy.protocols, 0, new_stop)
        .unwrap();
    assert_ne!(replacement.owner().identity(), old_instance);
    assert!(registry
        .release_closed(replay, |_| {
            published.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        })
        .is_err());
    assert_eq!(published.load(AtomicOrdering::SeqCst), 1);
    drop((installed, owner, group, host));
    assert!(
        weak_owner.upgrade().is_none(),
        "terminal/source must not retain the resource owner"
    );
    registry
        .shutdown("recovery", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
