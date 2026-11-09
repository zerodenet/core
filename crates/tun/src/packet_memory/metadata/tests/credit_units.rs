use super::*;
use zero_traits::packet_metadata::PacketMetadataCounter;
mod waker;
#[test]
fn same_credit_scalar_growth_and_refund_never_clone_drop_or_wake_its_generic_waker() {
    let one = PacketMetadataLayout::new(1, 8);
    let second = PacketMetadataLayout::new(1, 16);
    let counter =
        PacketMetadataCounter::new_owned(charged(one.checked_add(second).unwrap())).unwrap();
    let events = Arc::new(waker::Events::default());
    let mut credit = counter.reserve(one, waker::original(&events)).unwrap();
    let observed = events.snapshot();
    counter.grow_credit(&mut credit, second).unwrap();
    assert_eq!(credit.layout(), one.checked_add(second).unwrap());
    counter.shrink_credit(&mut credit, one).unwrap();
    assert_eq!(credit.layout(), second);
    assert_eq!(counter.snapshot().occupied, charged(second));
    assert_eq!(events.snapshot(), observed);
    drop(credit);
    assert_eq!(
        events.snapshot(),
        (observed.0, observed.1 + 1, observed.2 + 1)
    );
    assert_eq!(counter.snapshot().occupied, owned());
}
#[test]
fn refused_foreign_and_overflow_scalar_changes_preserve_exact_original_credit_and_both_books() {
    let one = PacketMetadataLayout::new(1, 8);
    let a = PacketMetadataCounter::new_owned(charged(one)).unwrap();
    let b = PacketMetadataCounter::new_owned(charged(one)).unwrap();
    let mut credit = a.reserve(one, std::task::Waker::noop().clone()).unwrap();
    let before = (a.snapshot(), b.snapshot(), credit.layout());
    assert_eq!(
        b.grow_credit(&mut credit, one),
        Err(PacketMemoryError::Stopped)
    );
    assert_eq!(
        b.shrink_credit(&mut credit, one),
        Err(PacketMemoryError::Stopped)
    );
    assert_eq!(
        a.shrink_credit(&mut credit, PacketMetadataLayout::new(2, 8)),
        Err(PacketMemoryError::Capacity)
    );
    assert_eq!((a.snapshot(), b.snapshot(), credit.layout()), before);
    let large = PacketMetadataCounter::new_owned(PacketMetadataLayout::new(usize::MAX, usize::MAX))
        .unwrap();
    let baseline = large.snapshot().occupied;
    let reserved = PacketMetadataLayout::new(1, usize::MAX - baseline.bytes());
    let mut credit = large
        .reserve(reserved, std::task::Waker::noop().clone())
        .unwrap();
    let before = (large.snapshot(), credit.layout());
    assert_eq!(
        large.grow_credit(
            &mut credit,
            PacketMetadataLayout::new(0, baseline.bytes() + 1)
        ),
        Err(PacketMemoryError::Overflow)
    );
    assert_eq!((large.snapshot(), credit.layout()), before);
}
#[test]
fn actual_root_growth_keeps_old_and_new_boxes_paid_and_notifies_only_after_owner_unlock() {
    use std::{
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
        task::{Context, Wake, Waker},
    };
    struct Reenter {
        root: Arc<PacketReturnTotals>,
        owner: Arc<Mutex<()>>,
        wakes: AtomicUsize,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            assert!(self.owner.try_lock().is_ok());
            let _ = self.root.driver_metadata();
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let old_layout = PacketMetadataLayout::new(1, 8);
    let new_layout = PacketMetadataLayout::new(1, 16);
    let root = original(old_layout.checked_add(new_layout).unwrap());
    let view = root.driver_metadata_view();
    let mut credit = view.reserve(old_layout).unwrap();
    let old = Box::new([7u8; 8]);
    view.grow_credit(&mut credit, new_layout).unwrap();
    let new = Box::new([9u8; 16]);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(old_layout.checked_add(new_layout).unwrap())
    );
    assert_eq!(*old, [7; 8]);
    let owner = Arc::new(Mutex::new(()));
    let entered = Arc::new(Reenter {
        root: root.clone(),
        owner: owner.clone(),
        wakes: AtomicUsize::new(0),
    });
    let wake = Waker::from(entered.clone());
    let mut context = Context::from_waker(&wake);
    let notified = root.changed.notified();
    tokio::pin!(notified);
    assert!(notified.as_mut().poll(&mut context).is_pending());
    let notifications = view.notifications();
    let locked = owner.lock().unwrap();
    drop(old);
    view.shrink_credit(&mut credit, old_layout).unwrap();
    assert_eq!(root.driver_metadata().occupied, charged(new_layout));
    assert_eq!(entered.wakes.load(Ordering::SeqCst), 0);
    drop(locked);
    drop(notifications);
    assert_eq!(entered.wakes.load(Ordering::SeqCst), 1);
    drop(new);
    view.shrink_credit(&mut credit, new_layout).unwrap();
    assert_eq!(root.driver_metadata().occupied, owned());
    view.grow_credit(&mut credit, old_layout).unwrap();
    assert_eq!(root.driver_metadata().occupied, charged(old_layout));
}
#[test]
fn actual_root_pause_rejects_growth_but_actual_freed_storage_can_refund_before_rollback() {
    let one = PacketMetadataLayout::new(1, 8);
    let root = original(one.checked_add(one).unwrap());
    let view = root.driver_metadata_view();
    let mut credit = view.reserve(one).unwrap();
    let before = root.driver_metadata();
    let target = before.limit.checked_add(one).unwrap();
    let pause = root
        .pause_driver_metadata_for_limit(target)
        .unwrap()
        .unwrap();
    assert_eq!(
        view.grow_credit(&mut credit, one),
        Err(PacketMemoryError::Paused)
    );
    assert_eq!(credit.layout(), one);
    view.shrink_credit(&mut credit, one).unwrap();
    assert_eq!(root.driver_metadata().occupied, owned());
    drop(pause);
    assert_eq!(root.driver_metadata().limit, before.limit);
    view.grow_credit(&mut credit, one).unwrap();
    assert_eq!(root.driver_metadata().occupied, before.occupied);
}
#[test]
fn ordinary_credit_growth_cannot_use_a_candidates_unpublished_increase() {
    let one = PacketMetadataLayout::new(1, 8);
    let counter = PacketMetadataCounter::new_owned(charged(one)).unwrap();
    let header = PacketMetadataCounter::new_owned(PacketMetadataLayout::new(32, 65536)).unwrap();
    let paid = header
        .reserve(
            PacketMetadataCounter::candidate_layout().unwrap(),
            std::task::Waker::noop().clone(),
        )
        .unwrap();
    let candidate = counter
        .prepare_limit(charged(PacketMetadataLayout::new(4, 128)), paid)
        .ok()
        .unwrap();
    let mut credit = counter
        .reserve(one, std::task::Waker::noop().clone())
        .unwrap();
    let before = (counter.snapshot(), credit.layout());
    assert!(matches!(
        counter.grow_credit(&mut credit, one),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert_eq!((counter.snapshot(), credit.layout()), before);
    candidate.rollback();
    counter.shrink_credit(&mut credit, one).unwrap();
    counter.grow_credit(&mut credit, one).unwrap();
    assert_eq!(credit.layout(), one);
}
