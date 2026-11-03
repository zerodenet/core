use super::retire_rejected_bindings;
use crate::runtime::{
    route_runtime::{InboundListenerRuntimeFactory, SharedIngressRuntimeServices},
    Proxy,
};
use std::{future::Future, num::NonZeroUsize, sync::Arc, task::Poll};
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn rejected_bind_original_closes_and_fault_is_published_before_recovery() {
    let (proxy, prepared) = candidate();
    let config = proxy.engine().config();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let lease = proxy
        .inbound_owners
        .reserve_listener(
            "failed",
            &config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();

    let owner = lease.owner();
    let identity = owner.identity();
    let mut original = lease.context().listen_tcp().unwrap();
    // The transferred original view is really suspended on accept. Its actual
    // native controller, not a rebound port, supplies the close receipt.
    let mut accept = Box::pin(original.accept_owned());
    std::future::poll_fn(|cx| {
        assert!(accept.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let source = tokio::net::TcpStream::connect(("127.0.0.1", 0))
        .await
        .unwrap_err();
    let failure = lease.reject(source.into());
    assert!(!failure.to_string().is_empty());
    drop(lease);
    assert!(proxy.inbound_owners.identities().contains(&identity));
    // The real pending native operation must observe revocation and relinquish
    // its unacquired reservation before cleanup can certify the original.
    assert!(timeout(Duration::from_secs(2), &mut accept)
        .await
        .unwrap()
        .is_err());
    drop(accept);
    let factory = InboundListenerRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        proxy.inbound_owners.clone(),
    );
    timeout(Duration::from_secs(2), retire_rejected_bindings(&factory))
        .await
        .unwrap()
        .unwrap();
    assert!(!proxy.inbound_owners.identities().contains(&identity));
    let terminal = owner.terminal_for_test().unwrap();
    assert_eq!(terminal.identity(), identity);
    assert_eq!(terminal.closed_io_receipts(), 1);
    assert_eq!(terminal.joined_tasks(), 0);
    assert!(terminal.failure().is_some());
    assert!(
        original.local_addr().is_err(),
        "the original transferred view is retired"
    );
    // Publication already consumed the fault. Re-entry neither republishes the
    // old source nor substitutes a fabricated completion for a live owner.
    retire_rejected_bindings(&factory).await.unwrap();
}

fn candidate() -> (Proxy, crate::protocol_registry::PreparedInboundBind) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(
            r#"{
        "inbounds":[{"tag":"failed","listen":{"address":"127.0.0.1","port":0},
          "udp":{"enabled":false},"protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"direct"}}
    }"#,
        )
        .unwrap(),
    )
    .unwrap();
    let config = proxy.engine().config();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(config.inbounds[0].clone(), None)
        .unwrap()
        .unwrap();
    (proxy, prepared)
}

#[tokio::test]
async fn cancelled_recovery_barrier_keeps_original_scope_until_real_io_finishes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (proxy, prepared) = candidate();
    let config = proxy.engine().config();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let lease = proxy
        .inbound_owners
        .reserve_listener(
            "failed",
            &config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();

    let owner = lease.owner();
    let identity = owner.identity();
    let original = lease.context().listen_tcp().unwrap();
    // This pending work belongs to the real Scope. The original controlled
    // listener retires separately; its own accepted reads cancel immediately.
    let gate = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(gate.local_addr().unwrap())
        .await
        .unwrap();
    let (mut stream, _) = gate.accept().await.unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let pending = entered.clone();
    owner
        .spawn_driver(async move {
            let read = stream.read_u8();
            tokio::pin!(read);
            let byte = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => {
                    pending.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await?;
            assert_eq!(byte, 42);
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    drop(lease);
    let factory = InboundListenerRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        proxy.inbound_owners.clone(),
    );
    let mut retiring = Box::pin(retire_rejected_bindings(&factory));
    std::future::poll_fn(|cx| {
        assert!(
            retiring.as_mut().poll(cx).is_pending(),
            "real task I/O remains pending"
        );
        Poll::Ready(())
    })
    .await;
    drop(retiring);
    assert!(owner.terminal_for_test().is_none());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    client.write_u8(42).await.unwrap();
    timeout(Duration::from_secs(2), retire_rejected_bindings(&factory))
        .await
        .unwrap()
        .unwrap();
    let terminal = owner.terminal_for_test().unwrap();
    assert_eq!(terminal.joined_tasks(), 1);
    assert_eq!(terminal.closed_io_receipts(), 1);
    assert!(original.local_addr().is_err());
    assert!(!proxy.inbound_owners.identities().contains(&identity));
}
