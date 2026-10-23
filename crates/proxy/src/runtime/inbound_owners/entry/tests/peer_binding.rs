use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn panic_before_acceptance() -> std::future::Ready<Result<(), EngineError>> {
    panic!("actual cold peer factory panic before scope acceptance");
}

#[tokio::test]
async fn peer_binding_publishes_only_the_real_dispatched_scope_receipt_and_original_failure() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, mut stopping) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "join-receipt",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (mut io, mut remote) = tokio::io::duplex(1);
    let pending = Arc::new(Notify::new());
    owner
        .spawn_driver({
            let pending = pending.clone();
            async move {
                assert!(group.spawn_bound(&host, |binding| {
                    binding_tx.send(binding.clone()).unwrap();
                    async move {
                        binding.wait_bound().await.map_err(std::io::Error::other)?;
                        assert!(!binding.cancellation().is_cancelled());
                        let read = io.read_u8();
                        tokio::pin!(read);
                        std::future::poll_fn(|cx| {
                            let result = read.as_mut().poll(cx);
                            if result.is_pending() {
                                pending.notify_one();
                            }
                            result
                        })
                        .await?;
                        panic!("actual scoped peer panic after its bound I/O suspension");
                    }
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                group.wait_closed().await
            }
        })
        .unwrap();
    let binding = binding_rx.await.unwrap();
    let task = binding.wait_bound().await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    remote.write_u8(1).await.unwrap();
    let exit = timeout(Duration::from_secs(2), owner.scope.lock().await.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.id, task);
    assert!(exit.result.is_err());
    // Even a real native join result is not this owner's dispatched receipt
    // until record() atomically binds its original ID/class/source.
    assert_eq!(
        binding
            .wait_joined(Instant::now() + Duration::from_millis(1))
            .await
            .unwrap_err()
            .reason,
        Reason::Timeout
    );
    owner.record(exit);
    let first = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    let retry = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &retry));
    assert_eq!(first.task(), task);
    assert_eq!(first.class(), TaskClass::Critical);
    assert!(Arc::ptr_eq(
        &first.failure().unwrap(),
        owner.failure.lock().unwrap().as_ref().unwrap()
    ));
    crate::runtime::inbound_owners::test_support::assert_tracked_panic(
        &binding,
        task,
        TaskClass::Critical,
        first.failure().as_ref().unwrap().as_ref(),
    );
    owner.stop();
    assert!(timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .is_err());
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
}

#[tokio::test]
async fn actual_cold_factory_panic_rejects_registration_without_poisoning_or_inventing_a_peer_join()
{
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, _) = watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "cold-panic",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let (send, receive) = tokio::sync::oneshot::channel();
    owner
        .spawn_driver({
            let group = group.clone();
            let host = host.clone();
            async move {
                group.spawn_bound(&host, |binding| {
                    send.send(binding).unwrap();
                    panic_before_acceptance()
                })?;
                Ok(())
            }
        })
        .unwrap();
    let binding = receive.await.unwrap();
    assert!(timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .is_err());
    assert!(binding.registration_rejection().is_some());
    assert!(binding.wait_bound().await.is_err());
    assert!(binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    assert!(!group
        .spawn(&host, || async {
            panic!("rejected group cannot start another task")
        })
        .unwrap());
    group.wait_closed().await.unwrap();
    assert_eq!(
        owner.report.lock().unwrap().exited_tasks,
        vec![*owner.driver.get().unwrap()]
    );
    assert_eq!(owner.report.lock().unwrap().failed_tasks.len(), 1);
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
}
