//! Exercise the ordinary SOCKS factory and actual native owner, not a marker.
use crate::Proxy;
use std::{future::Future, task::Context};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::{Duration, Instant},
};
use zero_core::{
    component::AssociationId,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::contract::datagram::DatagramRequest;
#[path = "opening/no_observer.rs"]
mod no_observer;

#[tokio::test]
async fn cancelled_native_association_preparation_retires_the_same_early_original() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":port}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap();
    let config = proxy.engine().runtime_snapshot().config().clone();
    let end = Instant::now() + Duration::from_secs(3);
    proxy
        .protocols
        .start_components(config.clone(), end)
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    let mut session = zero_core::Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        53,
        zero_core::Network::Udp,
        zero_core::ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    let execution = services.execution();
    let slot = execution
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    let physical = slot.original_carrier_owner(0).unwrap();
    assert!(physical.observe().unwrap().slots().is_empty());
    let reservation = execution.reserve_datagram().unwrap();
    let association = reservation.association();
    assert_ne!(association, AssociationId(0));
    let opening = reservation
        .begin_open(
            &execution,
            vec![slot.role_id()],
            execution.invocation.as_ref().unwrap().cancellation(),
        )
        .unwrap();
    let execution = execution.with_pending_datagram(opening.pending());
    let (greeting, seen) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut request = [0; 3];
        tcp.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [5, 1, 0]);
        greeting.send(()).unwrap();
        // No auth response: the actual awaited SOCKS handshake stays Pending.
        let mut bytes = Vec::new();
        tcp.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        tcp.shutdown().await.unwrap();
    });
    let original;
    {
        let mut operation = Box::pin(crate::component::stream_bridge::datagram::open(
            &execution,
            &slot,
            "node",
            Destination::new(Address::Ipv4([127, 0, 0, 1]), port),
            DatagramRequest {
                association,
                targets: DatagramTargets::Fixed(Destination::new(
                    Address::Ipv4([203, 0, 113, 9]),
                    53,
                )),
            },
        ));
        tokio::select! {
            result=&mut operation=>panic!("unanswered native handshake returned: {}",result.err().unwrap()),
            result=seen=>result.unwrap(),
        }
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(operation.as_mut().poll(&mut cx).is_pending());
        let actual = physical.observe().unwrap();
        assert_eq!(
            actual.slots().len(),
            1,
            "no UDP original before actual BND response"
        );
        assert!(actual.slots()[0].physical_view_live());
        assert!(actual.slots()[0].native().is_some());
        assert!(actual.slots()[0].driver().is_none());
        original = actual.slots()[0].original().clone();
        assert!(original.closed().is_none());
        // The same pending open future drops here, returning its actual view.
    }
    drop(opening);
    let retired = execution.retired_datagrams();
    let report = retired.progress_retired(end).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    let closed = original
        .closed()
        .expect("the original FD controller completed");
    assert!(original.matches_closed(&closed));
    assert!(
        report.exited_tasks.is_empty(),
        "taskless ownership invents no join"
    );
    peer.await.unwrap();
    proxy.protocols.shutdown_components(end).await.unwrap();
    assert!(physical.closed().unwrap().matches(&physical));
}
