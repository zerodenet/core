//! Real task panic, canonical delivery and actual Host ACK remain independent.
use super::*;
#[tokio::test]
async fn no_request_receiver_keeps_original_fault_until_canonical_delivery_and_host_ack() {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let owner = Arc::new(ProbeOwner::default());
        let target = Arc::new(WaitTarget::default());
        let mut request = Box::pin(owner.run(
            scope.registrar(),
            reserve_probe().await.unwrap(),
            one,
            ServicePurpose::DiagnosticProbe,
            Instant::now() + std::time::Duration::from_secs(2),
            |_| async {
                panic!("actual original probe ownership task");
                #[allow(unreachable_code)]
                ()
            },
        ));
        poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let receipt = scope.next_receipt().await.unwrap();
        let original = receipt.result().unwrap_err();
        assert_eq!(
            receipt.class(),
            zero_platform_tokio::contract::scope::TaskClass::Critical
        );
        owner.poll(&target);
        assert!(!receipt.joined().registration_released());
        let entry = lock(&owner.entries)[0].clone();
        assert!(
            lock(&entry.pending).is_empty(),
            "this real panic performed no native acquisition"
        );
        assert!(Arc::ptr_eq(entry.fault.get().unwrap(), &original));
        assert!(!entry.fault_received.load(Ordering::Acquire));
        let returned = owner.original_failure().unwrap();
        let same = std::error::Error::source(&returned)
            .unwrap()
            .downcast_ref::<ProbeOwnershipFailure>()
            .unwrap();
        assert!(Arc::ptr_eq(same.original(), &original));
        assert!(entry.fault_received.load(Ordering::Acquire));
        assert!(
            lock(&entry.credit).is_some(),
            "fault delivery alone is not actual Host ACK"
        );
        drop(request);
        scope.acknowledge(&receipt).unwrap();
        owner.poll(&target);
        assert!(entry.complete.load(Ordering::Acquire));
        assert!(lock(&entry.credit).is_none());
        assert!(lock(&owner.entries).is_empty());
        assert!(
            Arc::ptr_eq(same.original(), &original),
            "returned actual supervisor receiver retains source after reclaim"
        );
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
        let mut next = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        next.enable_registration().unwrap();
        let mut fresh = Box::pin(owner.run(
            next.registrar(),
            reserve_probe().await.unwrap(),
            one,
            ServicePurpose::DiagnosticProbe,
            Instant::now() + std::time::Duration::from_secs(2),
            |_| async { 7 },
        ));
        poll_fn(|cx| {
            assert!(fresh.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let actual = next.next_receipt().await.unwrap();
        actual.result().unwrap();
        next.acknowledge(&actual).unwrap();
        assert_eq!(
            poll_fn(|cx| {
                target.register(cx);
                owner.poll(&target);
                fresh.as_mut().poll(cx)
            })
            .await
            .unwrap(),
            7
        );
        next.begin_shutdown();
        assert!(next.next_receipt().await.is_none());
        assert!(next.is_drained());
    })
    .await
    .unwrap();
}
