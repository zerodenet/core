//! Real listener replacement consumes a closed old fault and restores last good config.
mod fixture;
use super::super::state::OrchestrationState;
use crate::runtime::Proxy;
use fixture::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Duration, Instant},
};
use zero_platform_tokio::contract::{lifecycle::erase_lifecycle, ChannelError};

#[tokio::test]
async fn cancelled_reconcile_retains_real_fault_and_native_owner_then_rebuilds_last_good_listener()
{
    prove_cancelled_recovery(false).await;
}
#[tokio::test]
async fn superseding_cancelled_candidate_hands_off_closed_fault_before_fresh_last_good_recovery() {
    prove_cancelled_recovery(true).await;
}
async fn prove_cancelled_recovery(supersede: bool) {
    let _trace = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .without_time()
            .with_test_writer()
            .finish(),
    );
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let candidate_echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = initial_listener(&proxy, &mut state, echo.local_addr().unwrap().port()).await;
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    round_trip(&client, address, &echo, b"last good before fault").await;
    let old = proxy.inbound_owners.owner("recover-direct").unwrap();
    let old_id = old.identity();
    let group = old.peer_tasks_for_test();
    let host = old.task_access();
    let ingress = proxy
        .tcp_runtime_services()
        .for_inbound("recover-direct".into())
        .execution()
        .ingress
        .unwrap()
        .role;
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut release_close = tokio::net::TcpStream::connect(gate.local_addr().unwrap())
        .await
        .unwrap();
    let (gate, _) = gate.accept().await.unwrap();
    let (address_tx, address_rx) = tokio::sync::oneshot::channel();
    let pending = Arc::new(tokio::sync::Notify::new());
    let closed = Arc::new(AtomicBool::new(false));
    let (mut io, mut panic_peer) = tokio::io::duplex(1);
    let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let claim_group = group.clone();
    assert!(group
        .spawn_bound(&host, {
            let pending = pending.clone();
            let closed = closed.clone();
            move |binding| {
                binding_tx.send(binding.clone()).unwrap();
                let context = claim_group.resource_context(binding, ingress);
                async move {
                    context
                        .claim(Instant::now() + Duration::from_secs(2), |_| {
                            Ok((
                                erase_lifecycle(NativeOwner {
                                    socket: None,
                                    address: Some(address_tx),
                                    gate,
                                    pending,
                                    closed,
                                }),
                                (),
                            ))
                        })
                        .await
                        .map_err(std::io::Error::other)?;
                    started_tx.send(()).unwrap();
                    io.read_u8().await?;
                    panic!("actual registered peer failure during configuration replacement");
                }
            }
        })
        .unwrap());
    started_rx.await.unwrap();
    let original_address = address_rx.await.unwrap();
    let binding = binding_rx.await.unwrap();
    let actual_task = binding.wait_bound().await.unwrap();
    let last_good = state.applied_snapshot.clone();
    let mut candidate = (**last_good.config()).clone();
    let zero_config::InboundProtocolConfig::Direct { port, .. } =
        &mut candidate.inbounds[0].protocol
    else {
        unreachable!()
    };
    *port = Some(candidate_echo.local_addr().unwrap().port());
    proxy.engine.stage_runtime_config(candidate).unwrap();
    panic_peer.write_u8(1).await.unwrap();
    let mut applying = Box::pin(state.reconcile_reload(&proxy));
    tokio::select! {
        _ = &mut applying => panic!("actual native resource close must remain pending"),
        _ = pending.notified() => {}
    }
    drop(applying);
    assert!(state.pending_dns_reload.is_some());
    assert!(!closed.load(Ordering::Acquire));
    assert!(UdpSocket::bind(original_address).await.is_err());
    assert_eq!(
        proxy
            .inbound_owners
            .owner("recover-direct")
            .unwrap()
            .identity(),
        old_id
    );
    if supersede {
        // Equal configuration values still produce a distinct actual host
        // candidate. It must reject/compensate the retained original token.
        proxy
            .engine
            .stage_runtime_config((*proxy.engine.config()).clone())
            .unwrap();
    }
    release_close.write_u8(1).await.unwrap();
    timeout(Duration::from_secs(5), state.reconcile_reload(&proxy))
        .await
        .unwrap();
    assert!(closed.load(Ordering::Acquire));
    let _confirmed = UdpSocket::bind(original_address).await.unwrap();
    let joined = binding
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(joined.task(), actual_task);
    assert_eq!(
        joined.class(),
        zero_platform_tokio::contract::scope::TaskClass::Critical
    );
    let original_failure = joined.failure().unwrap();
    crate::runtime::inbound_owners::test_support::assert_tracked_panic(
        &binding,
        actual_task,
        joined.class(),
        original_failure.as_ref(),
    );
    let replacement = proxy.inbound_owners.owner("recover-direct").unwrap();
    assert_ne!(replacement.identity(), old_id);
    assert!(Arc::ptr_eq(&state.applied_snapshot, &last_good));
    assert!(Arc::ptr_eq(&proxy.engine.runtime_snapshot(), &last_good));
    let observed = timeout(Duration::from_secs(2), state.listeners.join_next())
        .await
        .unwrap();
    assert_eq!(state.expected_listener_exits, 0);
    let mut unrelated_expected_exits = 2;
    assert!(super::super::lifecycle::handle_listener_result(
        observed,
        false,
        &mut unrelated_expected_exits
    )
    .is_ok());
    assert_eq!(unrelated_expected_exits, 2);
    round_trip(&client, address, &echo, b"last good after real recovery").await;
    let mut bytes = [0; 1];
    assert!(timeout(
        Duration::from_millis(20),
        candidate_echo.recv_from(&mut bytes)
    )
    .await
    .is_err());
    state.finish(&proxy, Ok(())).await.unwrap();
}
