//! Real hosted join and ACK remain separate from a returned probe body.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
};
use zero_platform_tokio::contract::scope::TaskScope;

#[tokio::test]
async fn body_and_actual_join_do_not_refund_the_same_probe_before_host_ack() {
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
            |_| async { 42 },
        ));
        poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let receipt = scope.next_receipt().await.unwrap();
        receipt.result().unwrap();
        assert!(!receipt.joined().registration_released());
        owner.poll(&target);
        poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        {
            let entries = lock(&owner.entries);
            assert_eq!(entries.len(), 1, "N1 still retains this exact original");
            assert!(!entries[0].complete.load(Ordering::Acquire));
            assert!(
                lock(&entries[0].credit).is_some(),
                "actual global credit has not returned"
            );
        }
        scope.acknowledge(&receipt).unwrap();
        let result = poll_fn(|cx| {
            target.register(cx);
            owner.poll(&target);
            request.as_mut().poll(cx)
        })
        .await
        .unwrap();
        assert_eq!(result, 42);
        assert!(lock(&owner.entries).is_empty());
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}
