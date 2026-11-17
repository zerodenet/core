//! Stable cleanup receipts are accepted only with actual retained custody.
use super::*;
use crate::contract::{Impact, Reason, Stage};

fn original(label: &'static str) -> Arc<ChannelError> {
    let payload = Arc::new(std::io::Error::other(label));
    let receipt = CleanupFailureReceipt::new();
    assert!(receipt.bind_payload(payload.clone()));
    Arc::new(
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
            .with_source(payload)
            .with_cleanup_failure_receipt(receipt),
    )
}
#[test]
fn full_actual_receiver_keeps_foreign_receipt_and_source_in_its_producer() {
    let receiver = ResourceCleanupFailures::default();
    let first = original("actual first controller");
    let driver = original("actual driver controller");
    let shutdown = original("actual shutdown controller");
    assert!(receiver.try_receive(first.clone(), CleanupFailurePhase::Driver));
    assert!(receiver.try_receive(driver.clone(), CleanupFailurePhase::Driver));
    assert!(receiver.try_receive(shutdown.clone(), CleanupFailurePhase::Shutdown));
    let foreign = original("another actual controller still owns this source");
    assert!(!receiver.try_receive(foreign.clone(), CleanupFailurePhase::Driver));
    assert!(!foreign.cleanup_failure_receipt().unwrap().is_acknowledged());
    assert_eq!(
        Arc::strong_count(&foreign),
        1,
        "the producer retains the unaccepted source"
    );
    let retained: Vec<_> = receiver.failures().collect();
    assert_eq!(retained.len(), 3);
    for actual in [&first, &driver, &shutdown] {
        assert!(retained.iter().any(|saved| Arc::ptr_eq(saved, actual)));
        assert!(actual.cleanup_failure_receipt().unwrap().is_acknowledged());
    }
    assert!(receiver.try_receive(driver.clone(), CleanupFailurePhase::Driver));
    assert!(!receiver.try_receive(foreign.clone(), CleanupFailurePhase::Shutdown));
    assert!(!foreign.cleanup_failure_receipt().unwrap().is_acknowledged());
    assert_eq!(Arc::strong_count(&foreign), 1);
}
