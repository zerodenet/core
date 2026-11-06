use super::*;
use crate::controlled_datagram::{BoundDatagramTotalsBinding, ControlledDatagramOwner};
use std::future::Future;
#[tokio::test]
async fn bound_original_owner_loss_during_real_pending_receive_keeps_native_view_and_actual_scope_ack_debts(
) {
    timeout(Duration::from_secs(3), async {
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let stop = CancellationToken::new();
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let (begin, started) = tokio::sync::oneshot::channel();
        let (pending, reached) = tokio::sync::oneshot::channel();
        let actual_totals = totals.clone();
        let actual_stop = stop.clone();
        let (_, actual) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                let caller = started.await.unwrap();
                let binding = BoundDatagramTotalsBinding::prepare(
                    &actual_totals,
                    instance(),
                    &caller,
                    &actual_stop,
                )
                .unwrap();
                let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
                owner.bind_bound_totals(binding).unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let policy = zero_traits::DialPolicy::default();
                let selection = egress.select_for_peer_with_policy(remote, &policy).unwrap();
                let io = owner.acquire(remote, None, &policy, &selection).unwrap();
                let control = Arc::new(owner.take_control(&io).unwrap());
                io.send_to_addr(b"actual bound", remote).await.unwrap();
                drop(owner); // Root must still strongly own this same native Entry.
                let source = {
                    let mut bytes = [0; 64];
                    let receive = io.recv_from_addr(&mut bytes);
                    tokio::pin!(receive);
                    std::future::poll_fn(|cx| {
                        assert!(receive.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    assert!(pending.send(control).is_ok());
                    receive.await.unwrap_err()
                };
                assert_eq!(source.kind(), std::io::ErrorKind::NotConnected);
                drop(io); // Actual unique IO return, independent of native and ACK.
                Ok(())
            })
            .unwrap();
        begin.send(actual.clone()).unwrap();
        let mut wire = [0; 64];
        let (read, _) = peer.recv_from(&mut wire).await.unwrap();
        assert_eq!(&wire[..read], b"actual bound");
        let control = reached.await.unwrap();
        assert_eq!(totals.snapshot().charged, 1);
        assert!(!control.observe().native_closed);
        assert!(!actual.is_joined());
        stop.cancel();
        let collect = std::future::poll_fn(|cx| {
            totals.poll_cancelled_retirements(cx);
            if control.observe().native_closed {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        });
        let (receipt, ()) = tokio::join!(scope.next_receipt(), collect);
        let receipt = receipt.unwrap();
        assert!(receipt.result().is_ok() && actual.matches_joined(receipt.joined()));
        assert!(control.observe().native_closed);
        let native = control.try_close().unwrap().unwrap();
        assert!(native.matches(&control));
        assert!(!receipt.joined().registration_released());
        assert_eq!(
            totals.snapshot().charged,
            1,
            "actual native and view returned, original Host ACK still owed"
        );
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&receipt).unwrap();
        assert_eq!(totals.snapshot().charged, 1);
        drop(deferred);
        let joined = actual.wait_registration_released().await;
        assert!(actual.matches_joined(&joined) && joined.registration_released());
        assert_eq!(totals.snapshot().charged, 0);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
