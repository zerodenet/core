use super::*;
use std::sync::atomic::AtomicUsize;
use zero_platform_tokio::contract::{DeliveryState, RetryClass, Stage};

async fn actual_failed_batch() -> (Arc<Replacement>, DeclaredCloseReceipt, Arc<ComponentPlan>) {
    let catalog = ComponentCatalog::default();
    let (panicking, state) = native::task_slot(&catalog, true).await;
    let old = plan(vec![panicking]);
    let host = TestContext::new(old.order.iter().map(AsRef::as_ref));
    host.drive(old.start(deadline())).await.unwrap();
    let mut peer = state.peer.lock().unwrap().take().unwrap();
    peer.write_u8(1).await.unwrap();
    drop(peer);
    let target = catalog.prepare(config()).unwrap();
    let batch = Arc::new(Replacement::new(target, None, [old.clone()]).unwrap());
    let receipt = host.drive(batch.confirm(deadline())).await.unwrap();
    let original = receipt.0.failure().unwrap();
    assert_eq!(original.0.id, state.task.lock().unwrap().unwrap());
    assert_eq!(original.0.class, TaskClass::Critical);
    let error = crate::component::tests::source::<tokio::task::JoinError>(&original.0.error);
    assert!(error.is_panic());
    assert_eq!(error.id(), state.native_task.lock().unwrap().unwrap());
    drop(host);
    (batch, receipt, old)
}

#[tokio::test]
async fn actual_fault_receiver_can_reenter_same_batch_but_cannot_publish_twice() {
    let (batch, receipt, old) = actual_failed_batch().await;
    let replay = batch.confirm(deadline()).await.unwrap();
    let failure = receipt.0.failure().unwrap().clone();
    let calls = AtomicUsize::new(0);
    let mut supervisor = Vec::new();
    let reported = batch
        .release(receipt, |terminal| {
            assert!(
                !batch.released(),
                "receiver queries cannot retain the publication lock"
            );
            let busy = batch
                .release(replay, |_| panic!("reentrant publication executed"))
                .unwrap_err();
            assert_eq!(busy.reason, Reason::Capacity);
            assert_eq!(busy.delivery, DeliveryState::NotSubmitted);
            supervisor.extend(terminal.failures().iter().cloned());
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(batch.released());
    assert!(Arc::ptr_eq(&supervisor[0], &failure));
    assert!(Arc::ptr_eq(original_failure(&reported), &failure));
    assert!(failure.reported());
    let weak = Arc::downgrade(&old);
    drop(batch);
    drop(old);
    assert!(
        weak.upgrade().is_none(),
        "published fault does not retain old runtime owner"
    );
}

#[tokio::test]
async fn panic_after_real_receiver_acceptance_keeps_unknown_outcome_and_never_republishes() {
    let (batch, receipt, old) = actual_failed_batch().await;
    let failure = receipt.0.failure().unwrap().clone();
    let nonce = Arc::as_ptr(&receipt.0);
    let mut supervisor = Vec::new();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = batch.release(receipt, |terminal| {
            supervisor.extend(terminal.failures().iter().cloned());
            panic!("real receiver accepted before callback panic");
        });
    }));
    assert!(panic.is_err());
    assert!(!batch.released());
    assert!(Arc::ptr_eq(&supervisor[0], &failure));
    assert!(
        !failure.reported(),
        "unknown acknowledgement is not successful publication"
    );
    let weak = Arc::downgrade(&old);
    for _ in 0..2 {
        let retry = batch.confirm(deadline()).await.unwrap();
        assert_eq!(Arc::as_ptr(&retry.0), nonce);
        let uncertain = batch
            .release(retry, |_| panic!("unknown publication was replayed"))
            .unwrap_err();
        assert_eq!(uncertain.stage, Stage::Persist);
        assert_eq!(uncertain.delivery, DeliveryState::Unknown);
        assert_eq!(uncertain.retry, RetryClass::Never);
        assert!(Arc::ptr_eq(original_failure(&uncertain), &failure));
        assert!(!batch.released());
    }
    assert!(
        old.confirmed(&BTreeSet::from([failure.1])),
        "actual declared task closure remains confirmed"
    );
    assert!(Arc::ptr_eq(
        original_failure(&old.shutdown(deadline()).await.unwrap_err()),
        &failure
    ));
    drop(old);
    assert!(
        weak.upgrade().is_some(),
        "uncertain publication retains original owner"
    );
}
