//! Actual native Pending, std panic, Scope receipt and independent source data.
use super::*;
use std::{error::Error, num::NonZeroUsize, time::Duration};
#[tokio::test]
async fn actual_std_panic_hands_native_drain_sources_to_the_same_scope_receiver() {
    let mut scope = crate::contract::scope::TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1),
    );
    scope.enable_registration().unwrap();
    let path = PathBuf::from(format!(
        r"\\.\pipe\zero-original-panic-and-drain-{}",
        std::process::id()
    ));
    let owner = ControlledPipeOwner::new(path, scope.registrar());
    // Inject a concrete Win32 OS error into this original native ledger to
    // exercise independent drain-source delivery deterministically. This is
    // not fabricated completion of an IO operation or closure of any handle.
    let native_source = Arc::new(native::signal(0).unwrap_err());
    lock(&owner.0.native).retain_fault_for_test(native_source.clone());
    let weak = Arc::downgrade(&native_source);
    let view = owner.open().await.unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    tokio::time::timeout_at(end, async {
        while !owner.pending_io().0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.0.panic_pending.store(true, Ordering::Release);
    owner.0.wake_worker();
    let receipt = tokio::time::timeout_at(end, scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let witness = owner.worker().unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    let received = receipt.joined().result().unwrap_err();
    assert_eq!(received.reason, Reason::Internal);
    assert_eq!(received.impact, Impact::Component);
    let io = received
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    let original = io
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    let panic = original
        .get_ref()
        .unwrap()
        .downcast_ref::<WorkerPanic>()
        .unwrap();
    assert_eq!(
        lock(&panic.payload).downcast_ref::<&str>().copied(),
        Some("original pipe worker fault after real native Pending")
    );
    let drain = panic
        .drain
        .as_ref()
        .expect("same independent native drain sources");
    let sources = drain
        .get_ref()
        .unwrap()
        .downcast_ref::<PipeNativeFailures>()
        .unwrap();
    assert_eq!(sources.errors().len(), 1);
    assert!(Arc::ptr_eq(&sources.errors()[0], &native_source));
    assert!(owner.std_worker_joined());
    assert_eq!(owner.pending_io(), (false, false, false));
    assert!(!receipt.joined().registration_released());
    assert!(owner.closed().is_none());
    assert!(owner.close(Instant::now()).await.is_err());
    drop(view);
    let closed = owner.close(end).await.unwrap();
    assert!(owner.matches_closed(&closed) && closed.had_native_acquisition());
    assert!(!receipt.joined().registration_released());
    scope.acknowledge(&receipt).unwrap();
    assert!(witness.try_joined().unwrap().registration_released());
    drop((scope, receipt, witness, owner, native_source));
    assert!(weak.upgrade().is_some());
    drop(received);
    assert!(weak.upgrade().is_none());
}
