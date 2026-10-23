//! Cancellation returns the original observer ownership, never its Scope receipts.
mod live;
mod native;
use super::super::state::OrchestrationState;
use crate::runtime::Proxy;
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
    retained.retain_failure(observer.1.unwrap_err());
    let forwarded = retained.take_failure().unwrap();
    assert!(native::contains_original_channel(
        &forwarded,
        original_failure.as_ref()
    ));
    drop(forwarded); // Consuming the fault cannot clear pending native retirement.
    assert!(retained.is_empty());
    drop(retained);
    let mut retained = registry.take_listener_observers().unwrap();
    assert!(retained.is_empty());
    assert!(
        retained.has_previous_generation(),
        "actual all-joins cannot erase pending native retirement"
    );
    assert!(registry.confirm_observers_retired(&mut retained).is_err());
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
