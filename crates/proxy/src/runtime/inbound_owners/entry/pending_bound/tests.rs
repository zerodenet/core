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
        watch::channel(false).0,
    )
    .await
    .unwrap();
    let crate::runtime::listeners::PendingInbound::Leased(lease) = &bound else {
        panic!("Direct TCP retains its actual preinstalled native owner");
    };
    let address = lease.local_addr().unwrap();
    let owner = lease.owner();
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
    assert_eq!(
        owner.resources.lock().unwrap().len(),
        1,
        "the actual native provider was installed before bind"
    );
    assert!(observers.is_empty());
    assert!(!owner.confirm_physical_close());
    assert!(TcpListener::bind(address).await.is_err());
    // The actual declared original, not a legacy bound-tree conversion, owns
    // close. Cancelling its cleanup cannot substitute for the same Scope join.
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
    assert!(
        proxy.inbound_owners.owner("late-listener").is_none(),
        "a stopped reserved source must never be published active"
    );
    assert!(proxy.inbound_owners.identities().contains(&identity));
    drop(locked);
    timeout(
        Duration::from_secs(2),
        owner.shutdown(Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap()
    .unwrap();
    let terminal = owner.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none());
    assert!(owner.scope_closed.load(Ordering::Acquire));
    assert_eq!(terminal.joined_tasks(), 0);
    assert_eq!(terminal.closed_io_receipts(), 1);
    assert!(terminal.task_failures().is_empty());
    assert!(terminal.native_bound_failures().is_empty());
    owner
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&terminal, &owner.terminal_for_test().unwrap()));
    proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            "late-listener",
            Instant::now() + Duration::from_secs(2),
            |published| {
                assert_eq!(published.identity(), identity);
                assert_eq!(published.joined_tasks(), 0);
                assert_eq!(published.closed_io_receipts(), 1);
                Ok(())
            },
        )
        .await
        .unwrap();
    assert!(!proxy.inbound_owners.identities().contains(&identity));
    // Repeating the old close proof cannot close a new socket using that port.
    let (client, accepted) = tokio::join!(
        tokio::net::TcpStream::connect(address),
        replacement.accept()
    );
    assert!(client.is_ok());
    assert!(accepted.is_ok());
}
