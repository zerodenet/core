//! Cancellation returns the original observer ownership, never its Scope receipts.
mod native;
use super::super::state::OrchestrationState;
use crate::runtime::Proxy;
use native::original_no_inbounds;
use std::{future::Future, sync::Arc, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{oneshot, Notify},
    time::{timeout, Duration, Instant},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{lifecycle::erase_lifecycle, scope::TaskClass};

async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let remote = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    (listener.accept().await.unwrap().0, remote)
}

#[tokio::test]
async fn interrupted_state_keeps_original_observer_joining_active_peer_and_separate_native_close() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    assert!(OrchestrationState::assemble(&proxy).is_err());
    let registry = proxy.inbound_owners.clone();
    let installed = registry
        .install_listener(
            "retained-peer",
            &proxy.protocols,
            1,
            state.shutdown_tx.clone(),
        )
        .unwrap();
    let owner = installed.owner();
    let group = installed.peer_tasks();
    let host = installed.tasks();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("retained-peer".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let (mut io, mut remote) = pair().await;
    let (gate, mut close_gate) = pair().await;
    let pending = Arc::new(Notify::new());
    let native_pending = Arc::new(Notify::new());
    let (address_tx, address_rx) = oneshot::channel();
    let (binding_tx, binding_rx) = oneshot::channel();
    let mut stopping = state.shutdown_tx.subscribe();
    owner
        .spawn_driver({
            let pending = pending.clone();
            let native_pending = native_pending.clone();
            async move {
                let claiming = group.clone();
                assert!(group.spawn_bound(&host, |binding| {
                    let context = claiming.resource_context(binding.clone(), ingress);
                    async move {
                        binding.wait_bound().await.map_err(std::io::Error::other)?;
                        context
                            .claim(Instant::now() + Duration::from_secs(2), |_| {
                                Ok((
                                    erase_lifecycle(native::NativeGate {
                                        socket: None,
                                        address: Some(address_tx),
                                        gate,
                                        pending: native_pending,
                                        gate_done: false,
                                    }),
                                    (),
                                ))
                            })
                            .await
                            .map_err(std::io::Error::other)?;
                        binding_tx.send(binding).unwrap();
                        let mut bytes = [0];
                        let read = io.read(&mut bytes);
                        tokio::pin!(read);
                        std::future::poll_fn(|cx| {
                            let result = read.as_mut().poll(cx);
                            if result.is_pending() {
                                pending.notify_one();
                            }
                            result
                        })
                        .await?;
                        Err(EngineError::NoInbounds)
                    }
                })?);
                let _ = stopping.wait_for(|stop| *stop).await;
                Ok(())
            }
        })
        .unwrap();
    owner.seal_listener_installation();
    let observing = owner.clone();
    let original_observer = state.listeners.spawn(async move {
        observing
            .observe_listener()
            .await
            .map_err(|error| observing.observer_error(error))
    });
    let binding = binding_rx.await.unwrap();
    let task = binding.wait_bound().await.unwrap();
    let original_native = address_rx.await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    assert!(UdpSocket::bind(original_native).await.is_err());
    drop(state); // Actual OrchestrationState cancellation and original stop propagation.
    assert!(registry.owner("retained-peer").is_some());
    timeout(Duration::from_secs(2), binding.cancellation().cancelled())
        .await
        .unwrap();
    assert!(
        binding.observed_join().is_none(),
        "stop and primary exit do not join pending native I/O"
    );
    assert!(owner.terminal_for_test().is_none());

    // Recovery borrows the same original set and cannot build a new generation
    // while the actual old peer is Pending. Cancelling recovery returns it again.
    let mut resumed = OrchestrationState::assemble(&proxy).unwrap();
    assert!(resumed.listeners.has_previous_generation());
    let mut recovery = Box::pin(resumed.start(&proxy));
    std::future::poll_fn(|cx| {
        assert!(recovery.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(recovery);
    drop(resumed);
    assert!(binding.observed_join().is_none());
    remote.shutdown().await.unwrap();
    let joined = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(joined.task(), task);
    assert_eq!(joined.class(), TaskClass::Critical);
    let original_failure = joined.failure().unwrap();
    let mut retained = registry.take_listener_observers().unwrap();
    let observer = timeout(Duration::from_secs(2), retained.join_next_with_id())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(observer.0, original_observer.id());
    assert!(observer.1.is_err());
    assert!(retained.is_empty());
    assert!(
        owner.terminal_for_test().is_none(),
        "all actual joins do not close native resources"
    );
    assert!(UdpSocket::bind(original_native).await.is_err());

    let mut close = Box::pin(
        registry.confirm_shutdown("retained-peer", Instant::now() + Duration::from_secs(2)),
    );
    tokio::select! { biased;
        result = &mut close => panic!("actual native shutdown must be Pending: {result:?}"),
        _ = native_pending.notified() => {},
    }
    drop(close);
    // Cancel the borrowed close future, then resume the exact original owner.
    close_gate.write_u8(1).await.unwrap();
    let receipt = registry
        .confirm_shutdown("retained-peer", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert!(Arc::ptr_eq(
        &receipt.terminal().failure().unwrap(),
        &original_failure
    ));
    let fault = &receipt.terminal().task_failures()[0];
    assert_eq!(fault.id, task);
    assert_eq!(fault.class, TaskClass::Critical);
    assert!(Arc::ptr_eq(&fault.source, &original_failure));
    assert!(registry
        .release_closed_reported(receipt, |_| Ok(()))
        .is_err());
    assert!(registry.owner("retained-peer").is_none());
    let _rebound = UdpSocket::bind(original_native).await.unwrap();
}

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
        super::super::lifecycle::run_loop(&proxy, &mut state, std::future::pending()),
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
