use super::*;
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::CancellationToken;
fn binding() -> UpstreamBinding {
    UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(3),
    }
}
fn demand() -> CarrierContext {
    CarrierContext {
        session: None,
        deadline: Some(Instant::now() + Duration::from_secs(5)),
        cancel: CancellationToken::new(),
    }
}
fn io_kind(error: &RuntimeError) -> std::io::ErrorKind {
    match error {
        RuntimeError::Io(error) => error.kind(),
        _ => panic!("expected actual I/O error"),
    }
}
#[tokio::test]
async fn explicit_context_does_not_fall_back_to_legacy_or_invent_a_default_demand() {
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let legacy = RelayStreamConnector::new(
        "legacy".into(),
        3,
        Arc::new(move |_, _| {
            invoked.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(std::io::Error::other("legacy callback source").into()) })
        }),
    );
    let error = legacy
        .connect_with_context(&binding(), "127.0.0.1".into(), 1234, demand())
        .await
        .err()
        .unwrap();
    assert_eq!(io_kind(&error), std::io::ErrorKind::Unsupported);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let error = legacy
        .connect("127.0.0.1".into(), 1234)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("legacy callback source"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let bound = RelayStreamConnector::new_service_bound(
        "bound".into(),
        binding(),
        ("127.0.0.1".into(), 1234),
        Arc::new(move |_, _, _| panic!("no implicit demand")),
    );
    let error = bound.connect("127.0.0.1".into(), 1234).await.err().unwrap();
    assert_eq!(io_kind(&error), std::io::ErrorKind::Unsupported);
}
#[tokio::test]
async fn full_binding_endpoint_and_preexpired_demand_are_checked_before_callback() {
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let bound = RelayStreamConnector::new_service_bound(
        "bound".into(),
        binding(),
        ("127.0.0.1".into(), 1234),
        Arc::new(move |_, _, _| {
            invoked.fetch_add(1, Ordering::SeqCst);
            panic!("rejected demand must not invoke opener")
        }),
    );
    let mut wrong = binding();
    wrong.dial.address_family = zero_traits::AddressFamily::OnlyIpv4;
    assert!(bound
        .connect_with_context(&wrong, "127.0.0.1".into(), 1234, demand())
        .await
        .is_err());
    wrong = binding();
    wrong.network_generation = NetworkGeneration(4);
    assert!(bound
        .connect_with_context(&wrong, "127.0.0.1".into(), 1234, demand())
        .await
        .is_err());
    wrong = binding();
    wrong.prefix.push(zero_core::component::RoleInstanceId {
        component: zero_core::component::ComponentId(1),
        role: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(1)),
        generation: zero_core::component::ResourceGeneration(1),
    });
    assert!(bound
        .connect_with_context(&wrong, "127.0.0.1".into(), 1234, demand())
        .await
        .is_err());
    assert!(bound
        .connect_with_context(&binding(), "other.test".into(), 1234, demand())
        .await
        .is_err());
    assert!(bound
        .connect_with_context(&binding(), "127.0.0.1".into(), 4321, demand())
        .await
        .is_err());
    let cancelled = demand();
    cancelled.cancel.cancel();
    let error = bound
        .connect_with_context(&binding(), "127.0.0.1".into(), 1234, cancelled)
        .await
        .err()
        .unwrap();
    assert_eq!(io_kind(&error), std::io::ErrorKind::Interrupted);
    let mut expired = demand();
    expired.deadline = Some(Instant::now());
    let error = bound
        .connect_with_context(&binding(), "127.0.0.1".into(), 1234, expired)
        .await
        .err()
        .unwrap();
    assert_eq!(io_kind(&error), std::io::ErrorKind::TimedOut);
    let mut invalid = demand();
    invalid.session = Some(zero_core::component::SessionId(1));
    assert!(bound
        .connect_with_context(&binding(), "127.0.0.1".into(), 1234, invalid)
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn pending_actual_tcp_cancel_keeps_unknown_delivery_and_next_call_uses_new_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = seen.clone();
    let connector = RelayStreamConnector::new_service_bound(
        "real-tcp".into(),
        binding(),
        (address.ip().to_string(), address.port()),
        Arc::new(move |host, port, context| {
            recorded.lock().unwrap().push(context.deadline);
            Box::pin(async move {
                let mut stream = TcpStream::connect((host.as_str(), port)).await?;
                stream.write_all(&[7]).await?;
                assert_eq!(stream.read_u8().await?, 9);
                Ok(TcpRelayStream::new(stream))
            })
        }),
    );
    let first = demand();
    let first_budget = first.deadline;
    let cancel = first.cancel.clone();
    let mut pending =
        connector.connect_with_context(&binding(), address.ip().to_string(), address.port(), first);
    let (mut peer, _) = tokio::select! {
        result = listener.accept() => result.unwrap(),
        result = &mut pending => panic!("carrier finished before peer: {}", result.err().unwrap()),
    };
    let marker = tokio::select! {
        result = peer.read_u8() => result.unwrap(),
        result = &mut pending => panic!("carrier finished before marker: {}", result.err().unwrap()),
    };
    assert_eq!(marker, 7);
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    cancel.cancel();
    let error = pending.await.err().unwrap();
    assert_eq!(io_kind(&error), std::io::ErrorKind::Interrupted);
    let RuntimeError::Io(ref outer) = error else {
        unreachable!()
    };
    let classified = outer
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    assert_eq!(classified.delivery, DeliveryState::Unknown);
    assert_eq!(classified.reason, Reason::Cancelled);
    drop(peer);
    let mut next = demand();
    next.deadline = Some(Instant::now() + Duration::from_secs(7));
    let next_budget = next.deadline;
    let open =
        connector.connect_with_context(&binding(), address.ip().to_string(), address.port(), next);
    let remote = async {
        let (mut peer, _) = listener.accept().await.unwrap();
        assert_eq!(peer.read_u8().await.unwrap(), 7);
        peer.write_all(&[9]).await.unwrap();
    };
    let (opened, ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(open, remote) })
            .await
            .unwrap();
    drop(opened.unwrap());
    assert_eq!(*seen.lock().unwrap(), vec![first_budget, next_budget]);
    assert!(cancel.is_cancelled());
}
#[derive(Debug)]
struct OriginalSource;
impl std::fmt::Display for OriginalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original carrier source")
    }
}
impl Error for OriginalSource {}
#[tokio::test]
async fn callback_failure_preserves_the_original_shared_source_object() {
    let original = Arc::new(OriginalSource);
    let emitted = original.clone();
    let connector = RelayStreamConnector::new_service_bound(
        "source".into(),
        binding(),
        ("127.0.0.1".into(), 1234),
        Arc::new(move |_, _, _| {
            let emitted = emitted.clone();
            Box::pin(async move { Err(RuntimeError::Io(std::io::Error::other(emitted))) })
        }),
    );
    let error = connector
        .connect_with_context(&binding(), "127.0.0.1".into(), 1234, demand())
        .await
        .err()
        .unwrap();
    let RuntimeError::Io(error) = error else {
        panic!("source class changed")
    };
    let preserved = error
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<OriginalSource>>()
        .unwrap();
    assert!(Arc::ptr_eq(preserved, &original));
}

#[path = "relay_connector/deadline.rs"]
mod deadline;
