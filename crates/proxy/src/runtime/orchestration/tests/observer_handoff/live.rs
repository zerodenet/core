use super::native::original_no_inbounds;
use super::*;

#[tokio::test]
async fn live_primary_fault_reaches_the_supervisor_before_the_original_peer_join() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let installed = proxy
        .inbound_owners
        .install_listener("live-fault", &proxy.protocols, 1, state.shutdown_tx.clone())
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let (mut io, mut remote) = pair().await;
    let pending = Arc::new(Notify::new());
    let (fail, failed) = oneshot::channel();
    let (binding_tx, binding_rx) = oneshot::channel();
    owner
        .spawn_driver({
            let pending = pending.clone();
            async move {
                assert!(group.spawn_bound(&host, |binding| async move {
                    binding.wait_bound().await.map_err(std::io::Error::other)?;
                    binding_tx.send(binding).unwrap();
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
                    Ok(())
                })?);
                failed.await.map_err(std::io::Error::other)?;
                Err(EngineError::NoInbounds)
            }
        })
        .unwrap();
    owner.seal_listener_installation();
    let observing = owner.clone();
    state.listeners.spawn(async move {
        observing
            .observe_listener()
            .await
            .map_err(|error| observing.observer_error(error))
    });
    let binding = binding_rx.await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    fail.send(()).unwrap();
    let error = timeout(
        Duration::from_secs(2),
        crate::runtime::orchestration::lifecycle::run_loop(
            &proxy,
            &mut state,
            std::future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(original_no_inbounds(&error), "original source: {error:?}");
    assert!(binding.observed_join().is_none());
    assert_eq!(
        state.listeners.len(),
        1,
        "live observer still owns the pending peer join"
    );
    drop(state);
    remote.write_u8(1).await.unwrap();
    let joined = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(joined.class(), TaskClass::Critical);
    let mut retained = proxy.inbound_owners.take_listener_observers().unwrap();
    let duplicate = timeout(Duration::from_secs(2), retained.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        crate::runtime::inbound_owners::consume_reported_error(&duplicate),
        "same actual instance's original fault was already received by the supervisor"
    );
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("live-fault", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    assert_eq!(receipt.terminal().task_failures().len(), 1);
    assert!(proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .is_err());
}
