//! Actual two-pool wire exhaustion and recovery; original native ACKs return credit.
use super::*;
mod fixture;
use fixture::*;
fn limits(physical: usize, logical: usize) -> PoolBudgets {
    PoolBudgets {
        pool_carriers: NonZeroUsize::new(1).unwrap(),
        pool_streams: NonZeroUsize::new(2).unwrap(),
        total_carriers: NonZeroUsize::new(physical).unwrap(),
        total_streams: NonZeroUsize::new(logical).unwrap(),
    }
}
#[tokio::test]
async fn actual_registry_budget_refuses_second_pool_until_first_native_users_and_original_task_ack_close(
) {
    timeout(Duration::from_secs(12), async {
        let (peer_a, stop_a, server_a) = server().await;
        let (peer_b, stop_b, server_b) = server().await;
        let root = Arc::new(PoolTotals::default());
        let options = Arc::new(root.options(limits(1, 1)).unwrap());
        let mut root_scope = scope();
        let actual_root = instance(920, 1);
        let mut budget_owner = owner(options.clone(), actual_root, root_scope.registrar());
        budget_owner.lifecycle.start(deadline()).await.unwrap();
        assert_eq!(root.charged_for_test(), (0, 0));
        let mut a = Fixture::cold(peer_a, 1, 2, None, None).with_totals(
            &options,
            actual_root,
            instance(921, 1),
        );
        let mut b = Fixture::cold(peer_b, 1, 2, None, None).with_totals(
            &options,
            actual_root,
            instance(922, 1),
        );
        a.start().await;
        b.start().await;
        assert_eq!(a.traffic.boundaries.load(Ordering::SeqCst), 0);
        assert_eq!(b.traffic.boundaries.load(Ordering::SeqCst), 0);
        let mut first = a.options.open(a.actual, a.demand()).await.unwrap();
        echo(&mut first).await;
        let original = a.managed().records.lock().unwrap()[0].clone();
        let witness = original.witness.lock().unwrap().clone().unwrap();
        assert_eq!(
            b.options
                .open(b.actual, b.demand())
                .await
                .err()
                .unwrap()
                .reason,
            Reason::Capacity,
            "same actual root logical budget spans pools"
        );
        let zero_platform_tokio::controlled_carrier::loan::StreamLoan {
            io,
            owner: mut control,
        } = first.into_stream_loan().unwrap();
        drop(io);
        assert!(control.close(deadline()).await.unwrap().is_complete());
        assert!(original.adoption.closed().is_none());
        assert!(!witness.is_joined());
        assert_eq!(
            b.options
                .open(b.actual, b.demand())
                .await
                .err()
                .unwrap()
                .reason,
            Reason::Capacity,
            "logical return cannot refund the still-live physical original"
        );
        assert_eq!(b.traffic.boundaries.load(Ordering::SeqCst), 0);
        assert!(b
            .managed()
            .grants
            .registration()
            .observe()
            .unwrap()
            .slots()
            .is_empty());
        let candidate = Arc::new(root.options(limits(2, 2)).unwrap());
        let mut candidate_scope = scope();
        let mut rejected = owner(
            candidate.clone(),
            instance(920, 2),
            candidate_scope.registrar(),
        );
        assert_eq!(
            rejected
                .lifecycle
                .start(deadline())
                .await
                .unwrap_err()
                .reason,
            Reason::Capacity,
            "cold budget proposal must not replace active old root"
        );
        assert!(rejected
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(candidate_scope.shutdown(deadline()).await.is_complete());
        let (native, tasks) = a.close().await;
        assert!(native.is_complete() && tasks.is_complete());
        assert!(original.closed());
        let joined = witness.wait_registration_released().await;
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        let native = original.adoption.closed().unwrap();
        assert!(native.matches(&original.adoption) && native.had_native_acquisition());
        assert!(original
            .adoption
            .users_closed()
            .unwrap()
            .matches(&original.adoption));
        let mut second = b.options.open(b.actual, b.demand()).await.unwrap();
        echo(&mut second).await;
        drop(second);
        let (native, tasks) = b.close().await;
        assert!(native.is_complete() && tasks.is_complete());
        assert!(budget_owner
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(root_scope.shutdown(deadline()).await.is_complete());
        let mut next_scope = scope();
        let mut next = owner(candidate, instance(920, 2), next_scope.registrar());
        next.lifecycle.start(deadline()).await.unwrap();
        assert!(next
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(next_scope.shutdown(deadline()).await.is_complete());
        stop_a.cancel();
        stop_b.cancel();
        server_a.await.unwrap();
        server_b.await.unwrap();
    })
    .await
    .unwrap();
}
