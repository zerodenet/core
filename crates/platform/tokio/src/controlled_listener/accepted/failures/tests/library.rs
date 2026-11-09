use super::*;
#[tokio::test]
async fn independent_n1_library_keeps_closed_fault_custody_until_its_actual_owner_takes_report() {
    timeout(Duration::from_secs(5), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
        let mut owner = listener(&scope);
        let mut io = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        for _ in 0..3 {
            let actual = reject(&mut scope, &mut owner, &mut io).await;
            assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
            let denied = rejected(io.accept_owned().await);
            assert_eq!(
                (denied.reason, denied.delivery),
                (Reason::Capacity, DeliveryState::NotSubmitted)
            );
            let received = owner.take_accepted_report();
            assert_eq!(received.failed_tasks.len(), 1);
            let fault = &received.failed_tasks[0];
            assert_eq!(fault.id, actual.id());
            assert!(Arc::ptr_eq(
                fault.error.retained_source().unwrap(),
                &actual.result().unwrap_err()
            ));
            assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
            assert!(owner.take_accepted_report().failed_tasks.is_empty());
        }
        drop(io);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
