use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::{
    net::UdpSocket,
    sync::{oneshot, watch, Notify},
    time::timeout,
};

#[tokio::test]
async fn never_entered_reservation_is_known_no_acquisition_but_entered_drop_never_returns_capacity()
{
    let mut owner = NativeObligations::new(1, 1, 1);
    for kind in [ChildKind::Tcp, ChildKind::Udp, ChildKind::Dns] {
        let reserved = owner.reserve(kind).unwrap();
        assert_eq!(owner.reserve(kind).err().unwrap().reason, Reason::Capacity);
        drop(reserved);
        let mut actual = owner.reserve(kind).unwrap();
        actual.enter().unwrap();
        drop(actual);
        assert_eq!(owner.reserve(kind).err().unwrap().reason, Reason::Capacity);
    }
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert_eq!(owner.0.lock().unwrap().records.len(), 3);
    assert_eq!(
        owner.reserve(ChildKind::Tcp).err().unwrap().reason,
        Reason::Revoked
    );
}

#[tokio::test]
async fn first_actual_failure_source_survives_repeated_shutdown() {
    use std::error::Error;
    let mut owner = NativeObligations::new(1, 1, 1);
    let mut permit = owner.reserve(ChildKind::Tcp).unwrap();
    permit.enter().unwrap();
    let error = permit.retain_error(std::io::Error::from_raw_os_error(10013));
    let mut cursor: &(dyn std::error::Error + 'static) = &error;
    loop {
        if cursor
            .downcast_ref::<std::io::Error>()
            .is_some_and(|source| source.raw_os_error() == Some(10013))
        {
            break;
        }
        cursor = cursor
            .source()
            .expect("returned error preserves the original typed OS source");
    }
    let original = owner.0.lock().unwrap().first_source.clone().unwrap();
    drop(permit);
    let error = owner.shutdown(Instant::now()).await.unwrap_err();
    assert!(error.source().is_some());
    let repeated = owner.shutdown(Instant::now()).await.unwrap_err();
    assert!(repeated.source().is_some());
    assert!(Arc::ptr_eq(
        &original,
        &owner.0.lock().unwrap().first_source.clone().unwrap()
    ));
    assert_eq!(owner.0.lock().unwrap().records.len(), 1);
}

#[tokio::test]
async fn actual_child_udp_drop_and_true_scope_join_cannot_complete_its_host_terminal() {
    use crate::{
        protocol_registry::PreparedInboundBind,
        runtime::{inbound_owners::InboundOwnerRegistry, Proxy},
    };
    use zero_config::RuntimeConfig;
    use zero_core::{request::Destination, Address};
    use zero_platform_tokio::contract::component::ResourceDeclaration;
    let proxy = Proxy::new(
        RuntimeConfig::parse(
            r#"{
        "inbounds":[{"tag":"actual","listen":{"address":"127.0.0.1","port":0},
            "udp":{"enabled":false},"protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"direct"}}
    }"#,
        )
        .unwrap(),
    )
    .unwrap();
    let config = proxy.engine().config();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let operation = proxy
        .protocols
        .prepare_inbound_listener(config.inbounds[0].clone(), None)
        .unwrap();
    let prepared = PreparedInboundBind::new(
        operation,
        Arc::from([ResourceDeclaration::TcpListener {
            accepted: None,
            destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        }]),
    )
    .unwrap();
    let (shutdown, _) = watch::channel(false);
    let mut lease = registry
        .reserve_listener(
            "actual",
            &config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.bind_native().unwrap();
    let (operation, view, installed) = lease.finish().unwrap().promote_native().unwrap();
    drop(operation);
    let gate = NativeObligations::new(1, 1, 1);
    installed.register(gate.resource()).unwrap();
    let owner = installed.owner();
    let mut obligation = gate.reserve(ChildKind::Udp).unwrap();
    let (address_sender, address_receiver) = oneshot::channel();
    let entered = Arc::new(Notify::new());
    let pending = entered.clone();
    let (resume, released) = oneshot::channel();
    owner
        .spawn_driver(async move {
            let _original_listener_view = view;
            obligation.enter().map_err(std::io::Error::other)?;
            let socket = UdpSocket::bind("127.0.0.1:0").await?;
            address_sender.send(socket.local_addr()?).unwrap();
            let mut buffer = [0; 32];
            let receive = socket.recv_from(&mut buffer);
            tokio::pin!(receive);
            std::future::poll_fn(|cx| {
                assert!(receive.as_mut().poll(cx).is_pending());
                pending.notify_one();
                Poll::Ready(())
            })
            .await;
            released.await.map_err(std::io::Error::other)?;
            // Actual Tokio UDP Drop still has no explicit native-close receipt.
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    let address = address_receiver.await.unwrap();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(UdpSocket::bind(address).await.is_err());
    resume.send(()).unwrap();
    assert!(registry
        .confirm_shutdown("actual", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    let probe = UdpSocket::bind(address).await.unwrap();
    assert!(owner.terminal_for_test().is_none());
    assert!(registry.owner("actual").is_some());
    assert_eq!(gate.0.lock().unwrap().records.len(), 1);
    assert!(
        gate.0
            .lock()
            .unwrap()
            .records
            .values()
            .next()
            .unwrap()
            .retiring
    );
    assert_eq!(probe.local_addr().unwrap(), address);
}

mod caller;

mod business;

mod carrier;
