//! An absent Host Root wakes Unknown; it cannot mint an accepted return.
use super::*;
use crate::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError,
};
#[tokio::test]
async fn accepted_source_observer_does_not_pin_root_or_certify_weak_expiry() {
    timeout(Duration::from_secs(5), async {
        let roots =
            crate::controlled_carrier::CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let address = "127.0.0.1:0".parse().unwrap();
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration(address, 1),
            NonZeroUsize::new(1).unwrap(),
            CancellationToken::new(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &scope.registrar(),
        )
        .unwrap();
        let instance = super::totals::instance(1);
        let packet = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let failures = crate::AcceptedTaskFailures::prepare(
            instance,
            &scope.registrar(),
            &packet.driver_metadata_view(),
        )
        .unwrap();
        original.bind_task_failure_receiver(&failures).unwrap();
        original
            .bind_original_totals(instance, &scope.registrar(), &roots)
            .unwrap();
        let mut listener = original.bind_listener(address).unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
        let (client, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept_owned()
        );
        let mut client = client.unwrap();
        let (accepted, _) = accepted.unwrap();
        let claim = accepted.claim();
        let lineage = receiver.carrier_origin(&claim).unwrap();
        assert_eq!(roots.snapshot().charged, 2);
        assert!(lineage.native_closed().is_none());
        let weak_root = Arc::downgrade(&roots);
        let mut waiting = Box::pin(lineage.wait_settled_original());
        poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(roots);
        assert!(
            weak_root.upgrade().is_none(),
            "the strong Source original has only a weak Root link"
        );
        let source = waiting.await.unwrap_err();
        assert_eq!(source.reason, Reason::Revoked);
        assert!(!lineage.is_settled());
        assert!(
            lineage.native_closed().is_none(),
            "weak expiry cannot close this original FD"
        );
        let (id, witness) = accepted
            .spawn_handler(
                &scope.registrar(),
                TaskClass::Auxiliary,
                |io, _| async move {
                    drop(io);
                    Ok::<(), ChannelError>(())
                },
            )
            .unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), id);
        assert!(witness.matches_joined(receipt.joined()));
        assert!(receiver.observe_joined(id).unwrap().result().is_ok());
        let closed = receiver.close(&claim, deadline()).await.unwrap().unwrap();
        assert!(closed.matches(&claim));
        assert!(!witness.try_joined().unwrap().registration_released());
        scope.acknowledge(&receipt).unwrap();
        assert!(lineage.native_closed().unwrap().matches(&claim));
        assert!(lineage.closed().unwrap().matches(&claim));
        assert!(
            !lineage.is_settled(),
            "no actual Root removal event occurred after losing that Root"
        );
        let mut byte = [0];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
        assert!(original
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        drop(listener);
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
