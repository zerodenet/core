//! Custody movement does not reserve/refund the actual counter a second time.
use super::*;
use zero_traits::packet_metadata::PacketMetadataCounter;
#[test]
fn same_counter_absorb_and_prepared_split_preserve_held_units_and_original_handles() {
    let one = PacketMetadataLayout::new(1, 17);
    let two = one.checked_add(one).unwrap();
    let counter = PacketMetadataCounter::new_owned(charged(two)).unwrap();
    let baseline = counter.snapshot().occupied;
    let mut left = counter
        .reserve(one, std::task::Waker::noop().clone())
        .unwrap();
    let right = counter
        .reserve(one, std::task::Waker::noop().clone())
        .unwrap();
    let held = counter.snapshot().occupied;
    let returned = left.absorb(right).ok().unwrap();
    assert_eq!(left.layout(), two);
    assert_eq!(returned.layout(), PacketMetadataLayout::new(0, 0));
    assert_eq!(counter.snapshot().occupied, held);
    drop(returned);
    assert_eq!(counter.snapshot().occupied, held);
    let mut split = left
        .split_prepared(one, std::task::Waker::noop().clone())
        .ok()
        .unwrap();
    assert!(left.same_counter(&split));
    assert_eq!(left.layout(), one);
    assert_eq!(counter.snapshot().occupied, held);
    let too_large = two.checked_add(one).unwrap();
    let (reason, wake) = split
        .split_prepared(too_large, std::task::Waker::noop().clone())
        .err()
        .unwrap();
    assert_eq!(reason, PacketMemoryError::Capacity);
    assert_eq!(split.layout(), one);
    assert!(wake.will_wake(std::task::Waker::noop()));
    assert_eq!(counter.snapshot().occupied, held);
    drop(wake);
    drop(split);
    assert_eq!(
        counter.snapshot().occupied,
        baseline.checked_add(one).unwrap()
    );
    drop(left);
    assert_eq!(counter.snapshot().occupied, baseline);
}
#[test]
fn foreign_counter_absorb_returns_the_original_credit_without_altering_either_book() {
    let one = PacketMetadataLayout::new(1, 13);
    let a = PacketMetadataCounter::new_owned(charged(one)).unwrap();
    let b = PacketMetadataCounter::new_owned(charged(one)).unwrap();
    let mut left = a.reserve(one, std::task::Waker::noop().clone()).unwrap();
    let right = b.reserve(one, std::task::Waker::noop().clone()).unwrap();
    let occupied_a = a.snapshot();
    let occupied_b = b.snapshot();
    let (reason, right) = left.absorb(right).err().unwrap();
    assert_eq!(reason, PacketMemoryError::Stopped);
    assert_eq!(left.layout(), one);
    assert_eq!(right.layout(), one);
    assert!(!left.same_counter(&right));
    assert_eq!(a.snapshot(), occupied_a);
    assert_eq!(b.snapshot(), occupied_b);
    drop(left);
    drop(right);
    assert_eq!(a.snapshot().occupied, owned());
    assert_eq!(b.snapshot().occupied, owned());
}
