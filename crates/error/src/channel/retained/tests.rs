use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use zero_traits::packet_metadata::PacketMetadataCounter;
#[derive(Debug)]
struct Original(Arc<AtomicBool>);
impl Drop for Original {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl fmt::Display for Original {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original")
    }
}
impl Error for Original {}
#[test]
fn paid_error_retains_the_same_header_until_its_last_weak_custodian() {
    let layout = RetainedError::metadata_layout().unwrap();
    let own = PacketMetadataCounter::owned_layout();
    let counter = PacketMetadataCounter::new_owned(own.checked_add(layout).unwrap()).unwrap();
    let freed = Arc::new(AtomicBool::new(false));
    let credit = counter
        .reserve(layout, std::task::Waker::noop().clone())
        .unwrap();
    let actual = RetainedError::from_prepaid(
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
            .with_source(Original(freed.clone())),
        credit,
    )
    .unwrap_or_else(|_| panic!("prepaid exact layout"));
    let weak = actual.downgrade();
    let same = weak.upgrade().unwrap();
    assert!(actual.ptr_eq(&same));
    assert!(actual.library_arc().is_none());
    assert_eq!(
        counter.snapshot().occupied,
        own.checked_add(layout).unwrap()
    );
    drop(actual);
    assert!(!freed.load(Ordering::Acquire));
    drop(same);
    assert!(freed.load(Ordering::Acquire));
    assert!(weak.upgrade().is_none());
    assert_eq!(
        counter.snapshot().occupied,
        own.checked_add(layout).unwrap()
    );
    drop(weak);
    assert_eq!(counter.snapshot().occupied, own);
}
#[test]
fn paid_receipt_owns_exact_payload_before_accepting_its_same_original() {
    let own = PacketMetadataCounter::owned_layout();
    let receipt_layout = CleanupFailureReceipt::metadata_layout().unwrap();
    let payload_layout = PacketMetadataArc::<Original>::layout().unwrap();
    let counter = PacketMetadataCounter::new_owned(
        own.checked_add(receipt_layout)
            .unwrap()
            .checked_add(payload_layout)
            .unwrap(),
    )
    .unwrap();
    let freed = Arc::new(AtomicBool::new(false));
    let payload = PacketMetadataArc::from_prepaid(
        Original(freed.clone()),
        counter
            .reserve(payload_layout, std::task::Waker::noop().clone())
            .unwrap(),
    )
    .unwrap_or_else(|_| panic!("exact payload"));
    let weak = PacketMetadataArc::downgrade(&payload);
    let receipt = CleanupFailureReceipt::from_prepaid(
        counter
            .reserve(receipt_layout, std::task::Waker::noop().clone())
            .unwrap(),
    )
    .unwrap_or_else(|_| panic!("exact receipt"));
    let erased = payload.into_error();
    assert!(receipt.bind_metadata_payload(erased.clone()));
    let actual = Arc::new(
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
            .with_metadata_source(erased.clone())
            .with_cleanup_failure_receipt(receipt.clone()),
    );
    assert!(!receipt.acknowledge_retained(&Arc::new(ChannelError::new(
        Stage::Close,
        Reason::Io,
        Impact::Component
    ))));
    assert!(receipt.acknowledge_retained(&actual));
    assert!(receipt.is_acknowledged());
    drop(actual);
    drop(erased);
    assert!(!freed.load(Ordering::Acquire));
    drop(receipt);
    assert!(freed.load(Ordering::Acquire));
    assert_eq!(
        counter.snapshot().occupied,
        own.checked_add(payload_layout).unwrap()
    );
    drop(weak);
    assert_eq!(counter.snapshot().occupied, own);
}
