use std::task::Waker;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataArc, PacketMetadataCounter};
#[test]
fn erased_strong_and_weak_views_keep_the_same_allocation_and_full_credit() {
    let owned = PacketMetadataCounter::owned_layout();
    let layout = PacketMetadataArc::<usize>::layout().unwrap();
    let limit = owned.checked_add(layout).unwrap();
    let counter = PacketMetadataCounter::new_owned(limit).unwrap();
    let wake = Waker::noop().clone();
    let original =
        PacketMetadataArc::from_prepaid(42usize, counter.reserve(layout, wake.clone()).unwrap())
            .ok()
            .unwrap();
    let erased = original.clone().into_any();
    let weak = PacketMetadataArc::downgrade(&erased);
    let roundtrip = weak.upgrade().unwrap().downcast::<usize>().ok().unwrap();
    assert!(PacketMetadataArc::ptr_eq(&original, &roundtrip));
    assert_eq!(*roundtrip, 42);
    assert_eq!(counter.snapshot().occupied, limit);
    drop(roundtrip);
    drop(erased);
    drop(original);
    assert!(weak.upgrade().is_none());
    assert_eq!(counter.snapshot().occupied, limit);
    assert!(matches!(
        counter.reserve(layout, wake.clone()),
        Err(PacketMemoryError::Busy)
    ));
    drop(weak);
    assert_eq!(counter.snapshot().occupied, owned);
    assert!(counter.reserve(layout, wake).is_ok());
}
#[test]
fn failed_erased_downcast_returns_the_original_with_its_credit() {
    let owned = PacketMetadataCounter::owned_layout();
    let layout = PacketMetadataArc::<usize>::layout().unwrap();
    let limit = owned.checked_add(layout).unwrap();
    let counter = PacketMetadataCounter::new_owned(limit).unwrap();
    let original = PacketMetadataArc::from_prepaid(
        7usize,
        counter.reserve(layout, Waker::noop().clone()).unwrap(),
    )
    .ok()
    .unwrap();
    let erased = original.clone().into_any();
    let rejected = erased.downcast::<String>().err().unwrap();
    assert_eq!(counter.snapshot().occupied, limit);
    let recovered = rejected.downcast::<usize>().ok().unwrap();
    assert!(PacketMetadataArc::ptr_eq(&original, &recovered));
    drop(original);
    drop(recovered);
    assert_eq!(counter.snapshot().occupied, owned);
}
