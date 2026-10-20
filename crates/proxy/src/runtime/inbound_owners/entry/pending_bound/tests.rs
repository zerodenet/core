//! The real synchronous spawn rejection keeps the original pre-bound listener.
mod errors;
use super::*;
use crate::runtime::{
    route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices},
    Proxy,
};
use std::{future::Future, task::Poll};
use tokio::{
    net::TcpListener,
    task::JoinSet,
    time::{timeout, Duration},
};
fn original(error: &EngineError) -> Arc<ChannelError> {
    let EngineError::Io(error) = error else {
        panic!("retained native ownership error")
    };
    let retained = error.get_ref().unwrap();
    if let Some(stopped) = retained.downcast_ref::<StoppedBeforeExecution>() {
        return stopped.0.clone();
    }
    let retained = retained.downcast_ref::<ChannelError>().unwrap();
    std::error::Error::source(retained)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap()
        .clone()
}
#[tokio::test]
async fn late_production_spawn_confirms_actual_tcp_close_and_zero_task_receipts_after_cancelled_cleanup(
) {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "inbounds":[{"tag":"late-listener","listen":{"address":"127.0.0.1","port":0},
            "udp":{"enabled":false},"protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let phase = Arc::new(crate::runtime::inbound_owners::PreparingInbounds::default());
    let factory = InboundListenerRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        proxy.inbound_owners.clone(),
    )
    .with_preparing(phase.clone());
    let bound = crate::runtime::listeners::bind_inbound_listener(
        &proxy.protocols,
        &factory,
        None,
        &config.inbounds[0],
    )
    .await
    .unwrap();
    let crate::protocol_registry::BoundInbound::Tcp(listener) = &bound else {
        panic!("actual TCP-only listener")
    };
    let address = listener.local_addr().unwrap();
    phase.begin_shutdown();
    let (stop, _) = watch::channel(false);
    let mut observers = JoinSet::new();
    let error = match crate::runtime::listeners::spawn_inbound_listener(
        &proxy.protocols,
        None,
        &factory,
        &config.inbounds[0],
        bound,
        stop,
        &mut observers,
    ) {
        Err(error) => error,
        Ok(_) => panic!("late preparation must be refused"),
    };
    let owner = proxy.inbound_owners.owner("late-listener").unwrap();
    let identity = owner.identity();
    let failure = original(&error);
    assert!(Arc::ptr_eq(
        owner.pending_native_cause.lock().unwrap().as_ref().unwrap(),
        &failure
    ));
    assert!(owner.failure.lock().unwrap().is_none());
    assert!(super::super::super::preparation_stopped(&error));
    assert_eq!(failure.component, Some(identity.component));
    assert_eq!(
        failure.delivery,
        zero_platform_tokio::contract::DeliveryState::Unknown
    );
    assert!(owner.driver.get().is_none());
    assert!(owner.resources.lock().unwrap().is_empty());
    assert!(observers.is_empty());
    assert!(!owner.confirm_physical_close());
    assert!(TcpListener::bind(address).await.is_err());
    let expired = owner.close_pending_bound(Instant::now()).await.unwrap_err();
    let expired = EngineError::Io(expired);
    assert!(Arc::ptr_eq(&original(&expired), &failure));
    assert!(!super::super::super::preparation_stopped(&expired));
    assert!(TcpListener::bind(address).await.is_err());
    assert!(owner.report.lock().unwrap().closed_io.is_empty());
    let locked = owner.scope.lock().await;
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    let replacement = TcpListener::bind(address).await.unwrap();
    assert_eq!(owner.report.lock().unwrap().closed_io.len(), 1);
    assert!(!owner.scope_closed.load(Ordering::Acquire));
    assert!(
        owner.terminal().is_none(),
        "native close alone does not confirm even the real empty Scope"
    );
    assert_eq!(
        proxy
            .inbound_owners
            .owner("late-listener")
            .unwrap()
            .identity(),
        identity
    );
    drop(locked);
    let receipt = timeout(
        Duration::from_secs(2),
        proxy
            .inbound_owners
            .confirm_shutdown("late-listener", Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(receipt.terminal().failure().is_none());
    assert!(owner.scope_closed.load(Ordering::Acquire));
    assert_eq!(*owner.joined_count.lock().unwrap(), 0);
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    assert!(owner.report.lock().unwrap().exited_tasks.is_empty());
    assert!(owner.report.lock().unwrap().failed_tasks.is_empty());
    assert_eq!(owner.report.lock().unwrap().closed_io.len(), 1);
    assert_eq!(receipt.terminal().joined_tasks(), 0);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert!(receipt.terminal().native_bound_failures().is_empty());
    let retry = proxy
        .inbound_owners
        .confirm_shutdown("late-listener", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&retry.terminal, &receipt.terminal));
    assert_eq!(retry.terminal().closed_io_receipts(), 1);
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |terminal| {
            assert!(terminal.failure().is_none());
            Ok(())
        })
        .unwrap();
    assert!(proxy.inbound_owners.owner("late-listener").is_none());
    // Repeating the old close proof cannot close a new socket using that port.
    let (client, accepted) = tokio::join!(
        tokio::net::TcpStream::connect(address),
        replacement.accept()
    );
    assert!(client.is_ok());
    assert!(accepted.is_ok());
}

#[cfg(feature = "managed-datagram-runtime")]
#[tokio::test]
async fn mixed_pending_native_tree_confirms_tcp_without_dropping_or_claiming_unsupported_udp() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let tcp = zero_platform_tokio::TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let tcp_address = tcp.local_addr().unwrap();
    let udp = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let udp_address = udp.local_addr().unwrap();
    let (stop, _) = watch::channel(false);
    let owner = proxy
        .inbound_owners
        .install_listener("mixed-cold", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    let error = owner.retain_unexecuted_bound(
        crate::protocol_registry::BoundInbound::TcpAndDatagram(tcp, udp),
        std::io::Error::other("actual candidate preparation was cancelled").into(),
    );
    let failure = original(&error);
    for _ in 0..2 {
        let error = proxy
            .inbound_owners
            .confirm_shutdown("mixed-cold", Instant::now() + Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(Arc::ptr_eq(&original(&error), &failure));
        assert!(owner.terminal().is_none());
        assert!(owner.scope_closed.load(Ordering::Acquire));
        assert_eq!(*owner.joined_count.lock().unwrap(), 0);
        assert_eq!(owner.report.lock().unwrap().closed_io.len(), 1);
        assert!(owner.report.lock().unwrap().exited_tasks.is_empty());
        assert!(tokio::net::UdpSocket::bind(udp_address).await.is_err());
        let _closed_tcp = TcpListener::bind(tcp_address).await.unwrap();
    }
    assert!(proxy.inbound_owners.owner("mixed-cold").is_some());
    // UDP remains the same original held Arc until its own native controller
    // is integrated. TCP confirmation never clears that independent Unknown.
}
