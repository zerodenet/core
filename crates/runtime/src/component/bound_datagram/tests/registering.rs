//! The ordinary Bound provider acquires the actual raw socket before Tokio IO.
use super::retirement::{declaration, instance, request};
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_bound_raw_registration_keeps_original_until_view_join_and_host_ack() {
    tokio::time::timeout(Duration::from_secs(3), check(false))
        .await
        .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_bound_creator_cannot_lose_or_rebuild_its_unregistered_native() {
    tokio::time::timeout(Duration::from_secs(3), check(true))
        .await
        .unwrap();
}
async fn check(cancel_during_registration: bool) {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let cancel = CancellationToken::new();
    let provider = HostBoundDatagramResources::new_scoped_with_totals(
        instance(),
        scope.registrar().scope_identity(),
        &declaration(),
        cancel.clone(),
        root.clone(),
    )
    .unwrap();
    let pause = provider.state().owner.pause_native_registration();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let request = request(&provider, address);
    let (start, started) = tokio::sync::oneshot::channel();
    let (ready, native_ready) = tokio::sync::oneshot::channel();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let producer = provider.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            started.await.unwrap();
            let result = producer.open_bound_outbound_datagram(request);
            if cancel_during_registration {
                let failure = result.unwrap_err();
                assert_eq!(failure.stage, Stage::Prepare);
                // Actual registration refused the retired Entry and closed its
                // exact original; no replacement acquisition is attempted.
                assert!(ready.send(None).is_ok());
                finished.await.unwrap();
            } else {
                let grant = result.unwrap();
                let original = grant.socket.original().unwrap();
                grant
                    .socket
                    .send_to_addr(b"original Bound socket", address)
                    .await
                    .unwrap();
                assert!(ready.send(Some(original)).is_ok());
                let mut bytes = [0; 64];
                let mut receive = Box::pin(grant.socket.recv_from_addr(&mut bytes));
                poll_fn(|cx| {
                    assert!(receive.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                finished.await.unwrap();
                grant.socket.close().unwrap();
                assert!(receive.await.is_err());
                drop(grant);
            }
            Ok(())
        })
        .unwrap();
    provider.activate_driver(&witness).unwrap();
    start.send(()).unwrap();
    let identity = pause.wait_acquired().await;
    let reservation = pause.reservation();
    assert!(pause.is_raw_and_unclosed());
    assert_eq!(root.snapshot().charged, 1);
    assert!(!witness.is_joined());
    poll_fn(|cx| {
        root.poll_cancelled_retirements(cx);
        Poll::Ready(())
    })
    .await;
    assert!(pause.is_raw_and_unclosed());
    if cancel_during_registration {
        cancel.cancel();
        poll_fn(|cx| {
            root.poll_cancelled_retirements(cx);
            Poll::Ready(())
        })
        .await;
        assert!(pause.try_close().is_none(), "real creator still owns setup");
        assert!(pause.is_raw_and_unclosed());
        assert_eq!(root.snapshot().charged, 1);
    }
    pause.resume();
    let original = native_ready.await.unwrap();
    if let Some(original) = &original {
        assert!(identity.same_original(original.owner().identity()));
        assert!(original.driver().same_registration(&witness));
        assert!(original.closed().is_none());
        let mut bytes = [0; 64];
        let (size, _) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"original Bound socket");
    } else {
        assert!(cancel_during_registration);
        assert_eq!(pause.closed().unwrap().reservation(), reservation);
    }
    assert_eq!(root.snapshot().charged, 1);
    finish.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    receipt.result().unwrap();
    assert!(witness.is_joined());
    assert!(!witness.try_joined().unwrap().registration_released());
    if let Some(original) = &original {
        let closed = original.closed().unwrap();
        assert!(closed.matches(original.owner()));
        assert_eq!(closed.receipt().reservation(), reservation);
    }
    // Native close and returned unique view are positive before the real ACK.
    assert_eq!(root.snapshot().charged, 1);
    scope.acknowledge(&receipt).unwrap();
    assert!(witness.try_joined().unwrap().registration_released());
    assert_eq!(root.snapshot().charged, 0);
    let report = provider.shutdown_io().unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io.len(),
        1,
        "one actual original, never rebuilt"
    );
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}
