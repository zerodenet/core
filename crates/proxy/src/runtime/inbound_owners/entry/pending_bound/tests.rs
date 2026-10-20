//! The real synchronous spawn rejection keeps the original pre-bound listener.
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
    let retained = error
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    std::error::Error::source(retained)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap()
        .clone()
}
#[tokio::test]
async fn late_production_spawn_retains_original_bound_and_unknown_across_cancelled_empty_scope_cleanup(
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
        owner.failure.lock().unwrap().as_ref().unwrap(),
        &failure
    ));
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
    let locked = owner.scope.lock().await;
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(TcpListener::bind(address).await.is_err());
    assert_eq!(
        proxy
            .inbound_owners
            .owner("late-listener")
            .unwrap()
            .identity(),
        identity
    );
    drop(locked);
    let error = timeout(
        Duration::from_secs(2),
        proxy
            .inbound_owners
            .confirm_shutdown("late-listener", Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(Arc::ptr_eq(&original(&error), &failure));
    assert!(owner.scope_closed.load(Ordering::Acquire));
    assert_eq!(*owner.joined_count.lock().unwrap(), 0);
    assert!(!owner.driver_finished.load(Ordering::Acquire));
    assert!(owner.report.lock().unwrap().exited_tasks.is_empty());
    assert!(owner.report.lock().unwrap().failed_tasks.is_empty());
    assert!(owner.report.lock().unwrap().closed_io.is_empty());
    assert!(
        owner.terminal().is_none(),
        "empty Scope is not a native listener close receipt"
    );
    let pending = owner.pending_native_bound.lock().unwrap();
    let Some(crate::protocol_registry::BoundInbound::Tcp(listener)) = pending.as_ref() else {
        panic!("same original bound must stay in its actual owner")
    };
    assert_eq!(listener.local_addr().unwrap(), address);
    drop(pending);
    let (stop, _) = watch::channel(false);
    assert!(proxy
        .inbound_owners
        .install("late-listener", &proxy.protocols, 0, stop)
        .is_err());
    let retry = proxy
        .inbound_owners
        .confirm_shutdown("late-listener", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(&original(&retry), &failure));
    assert!(TcpListener::bind(address).await.is_err());
    // No artificial close at teardown: this conservative guard intentionally
    // remains Unknown until the original listener gets an explicit controller.
}
