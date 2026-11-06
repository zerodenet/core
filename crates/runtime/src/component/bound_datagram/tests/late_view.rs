//! The lifecycle returns this one view after its actual wire already joined.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn late_returned_view_requires_same_original_harvest_after_wire_join_and_native_close() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let provider = HostBoundDatagramResources::new_scoped(
        retirement::instance(),
        scope.registrar().scope_identity(),
        &retirement::declaration(),
        CancellationToken::new(),
    )
    .unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = retirement::request(&provider, peer.local_addr().unwrap());
    let (transfer, take) = tokio::sync::oneshot::channel::<
        Arc<zero_platform_tokio::contract::component::BoundDatagramSocket>,
    >();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let view = take.await.unwrap();
            let mut bytes = [0; 64];
            let mut receive = Box::pin(view.recv_from_addr(&mut bytes));
            poll_fn(|cx| {
                assert!(receive.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            entered.send(()).unwrap();
            assert!(
                receive.await.is_err(),
                "same native close wakes the original pending IO"
            );
            Ok(())
        })
        .unwrap();
    provider.activate_driver(&witness).unwrap();
    let view = Arc::new(
        provider
            .open_bound_outbound_datagram(request)
            .unwrap()
            .socket,
    );
    let returned = view.return_observation();
    let origin = view.original().unwrap();
    transfer.send(view.clone()).unwrap();
    pending.await.unwrap();
    let closed = view.close().unwrap();
    assert_eq!(origin.closed().unwrap().receipt(), closed);
    let receipt = scope.next_receipt().await.unwrap();
    receipt.result().unwrap();
    assert!(witness.is_joined() && !witness.try_joined().unwrap().registration_released());
    assert!(
        !returned.is_returned(),
        "the same host still owns the unique view object"
    );
    let first = provider
        .shutdown_scoped_io(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!first.is_complete());
    assert!(!provider.evidence(0).physical_confirmed());
    assert_eq!(provider.state().originals.len(), 1);
    drop(view);
    assert!(returned.is_returned());
    assert!(
        !provider.evidence(0).physical_confirmed(),
        "a static pre-harvest snapshot cannot retire the old record"
    );
    assert_eq!(provider.state().originals.len(), 1);
    let harvested = provider.take_closed_io();
    assert!(harvested.is_complete());
    assert!(provider.evidence(0).physical_confirmed());
    assert!(
        !provider.retired(0),
        "native/view/join are independent of the original Host ACK"
    );
    scope.acknowledge(&receipt).unwrap();
    assert!(provider.retired(0));
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}
