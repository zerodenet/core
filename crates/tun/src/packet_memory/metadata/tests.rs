use super::*;
fn owned() -> PacketMetadataLayout {
    zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
}
fn charged(layout: PacketMetadataLayout) -> PacketMetadataLayout {
    owned().checked_add(layout).unwrap()
}
fn original(available: PacketMetadataLayout) -> Arc<PacketReturnTotals> {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    Arc::new(PacketReturnTotals::new_with_metadata(one, one, one, charged(available)).unwrap())
}
#[test]
fn a_surviving_weak_header_keeps_both_real_metadata_allocations_charged() {
    let layout = PacketMetadataArc::<usize>::layout().unwrap();
    let root = original(layout);
    let credit = root.reserve_driver_metadata(layout).unwrap();
    let actual = PacketMetadataArc::from_prepaid(42usize, credit)
        .ok()
        .unwrap();
    let weak = PacketMetadataArc::downgrade(&actual);
    let second = weak.upgrade().unwrap();
    assert!(PacketMetadataArc::ptr_eq(&actual, &second));
    assert_eq!(*second, 42);
    drop(second);
    drop(actual);
    assert!(weak.upgrade().is_none());
    assert_eq!(root.driver_metadata().occupied, charged(layout));
    assert!(matches!(
        root.reserve_driver_metadata(layout),
        Err(PacketMemoryError::Busy)
    ));
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, owned());
    assert!(root.reserve_driver_metadata(layout).is_ok());
}
#[test]
fn failed_construction_returns_the_unconsumed_original_and_same_credit() {
    let root = original(PacketMetadataArc::<usize>::layout().unwrap());
    let small = PacketMetadataLayout::new(1, 1);
    let credit = root.reserve_driver_metadata(small).unwrap();
    let (source, actual, credit) = PacketMetadataArc::from_prepaid(42usize, credit)
        .err()
        .unwrap();
    assert_eq!(source, PacketMemoryError::Capacity);
    assert_eq!(actual, 42);
    assert_eq!(credit.layout(), small);
    assert_eq!(root.driver_metadata().occupied, charged(small));
    drop(credit);
    assert_eq!(root.driver_metadata().occupied, owned());
}
#[test]
fn zero_sized_arrays_do_not_claim_an_allocation_and_arc_headers_still_do() {
    assert_eq!(
        PacketMetadataLayout::array::<()>(usize::MAX).unwrap(),
        PacketMetadataLayout::new(0, 0)
    );
    assert_eq!(
        PacketMetadataLayout::array::<u8>(0).unwrap(),
        PacketMetadataLayout::new(0, 0)
    );
    assert!(PacketMetadataLayout::arc::<()>().unwrap().bytes() >= 2 * std::mem::size_of::<usize>());
    assert_eq!(
        PacketMetadataLayout::array::<u64>(usize::MAX),
        Err(PacketMemoryError::Overflow)
    );
}
#[test]
fn actual_weak_custody_blocks_policy_shrink_without_installing_any_other_lane() {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    let layout = PacketMetadataArc::<usize>::layout().unwrap();
    let root = original(layout);
    let actual =
        PacketMetadataArc::from_prepaid(7usize, root.reserve_driver_metadata(layout).unwrap())
            .ok()
            .unwrap();
    let weak = PacketMetadataArc::downgrade(&actual);
    drop(actual);
    let next = PacketMemoryLimit {
        frames: 2,
        bytes: 2,
    };
    let small = charged(PacketMetadataLayout::new(1, 1));
    assert_eq!(
        root.install_original_limits(next, next, next, small),
        Err(PacketMemoryError::Busy)
    );
    assert_eq!(root.returns().limit, one);
    assert_eq!(root.native().limit, one);
    assert_eq!(root.transmit().limit, one);
    assert_eq!(root.driver_metadata().limit, charged(layout));
    drop(weak);
    root.install_original_limits(next, next, next, small)
        .unwrap();
    assert_eq!(root.returns().limit, next);
    assert_eq!(root.driver_metadata().occupied, owned());
}
#[test]
fn revoking_the_actual_read_view_seals_new_metadata_and_keeps_existing_weak_headers_charged() {
    let layout = PacketMetadataArc::<usize>::layout().unwrap();
    let root = original(layout);
    let read = root.native_grant(1).unwrap();
    let view = read.memory_view().unwrap();
    let actual =
        PacketMetadataArc::from_prepaid(3usize, view.reserve_driver_metadata(layout).unwrap())
            .ok()
            .unwrap();
    let weak = PacketMetadataArc::downgrade(&actual);
    read.revoke();
    assert!(matches!(
        view.reserve_driver_metadata(layout),
        Err(PacketMemoryError::Stopped)
    ));
    drop(actual);
    assert_eq!(root.driver_metadata().occupied, charged(layout));
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, owned());
}
#[test]
fn policy_that_cannot_hold_its_actual_counter_is_rejected_before_allocating_it() {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    assert!(matches!(
        PacketReturnTotals::new_with_metadata(one, one, one, PacketMetadataLayout::new(1, 1)),
        Err(PacketMemoryError::Capacity)
    ));
}
#[path = "tests/owner_box.rs"]
mod owner_box;

#[tokio::test]
async fn original_metadata_wait_observes_actual_refund_and_pause_release_without_another_owner() {
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    let layout = PacketMetadataLayout::boxed::<[u8; 32]>();
    let root = original(layout);
    let view = root.driver_metadata_view();
    let occupied = view.reserve(layout).unwrap();
    let waiting = view.reserve_owned(layout);
    tokio::pin!(waiting);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(occupied);
    let credit = match waiting.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(credit)) => credit,
        _ => panic!("the same Root refund completes its original waiter"),
    };
    drop(credit);
    let limit = view.snapshot().limit;
    let pause = root.pause_driver_metadata_for_limit(limit).unwrap();
    // Equal limits need no pause. A real new smaller installed policy does.
    drop(pause);
    let next = PacketMetadataLayout::new(limit.allocations(), limit.bytes() - 1);
    let pause = root.pause_driver_metadata_for_limit(next).unwrap().unwrap();
    let waiting = view.reserve_owned(PacketMetadataLayout::boxed::<u8>());
    tokio::pin!(waiting);
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(pause);
    assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
}

#[tokio::test]
async fn original_metadata_wait_rejects_an_impossible_layout_including_its_real_counter() {
    let root = original(PacketMetadataLayout::boxed::<u8>());
    let view = root.driver_metadata_view();
    assert!(matches!(
        view.reserve_owned(view.snapshot().limit).await,
        Err(PacketMemoryError::Capacity)
    ));
}

#[path = "tests/credit_transfer.rs"]
mod credit_transfer;
