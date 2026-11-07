use super::*;
#[test]
fn dequeued_nonstatistics_originals_still_hold_local_and_shared_pending_credit() {
    let registry = registry(SubscriberLimits {
        pending: 1,
        pending_total: 1,
        registrations: 1,
        ..Default::default()
    });
    let (rx, registration) = subscribe(&registry, Default::default());
    let root = registry.lock().unwrap().root.clone();
    send(&registry, 0);
    let held = rx.recv().unwrap();
    assert_eq!(root.events.load(Ordering::Acquire), 1);
    assert_eq!(root.stats.load(Ordering::Acquire), 0);
    send(&registry, 0);
    assert!(rx.try_recv().is_err());
    drop(registration); // Logical unregister, no new publication is needed.
    assert!(registry.lock().unwrap().is_empty());
    assert_eq!(root.events.load(Ordering::Acquire), 1);
    assert_eq!(root.registrations.load(Ordering::Acquire), 1);
    let mut low = registry.lock().unwrap().current_limits();
    low.registrations = 1;
    let prepared = PreparedSubscriber::prepare(&registry, Default::default());
    assert_eq!(prepared.err().unwrap().kind(), io::ErrorKind::WouldBlock);
    let shared = held.event.clone();
    let moved = crate::RetainedEvent::from_queued(held).into_shared();
    assert!(Arc::ptr_eq(&shared, &moved));
    assert_eq!(root.events.load(Ordering::Acquire), 0);
    assert_eq!(root.registrations.load(Ordering::Acquire), 0);
    drop(rx);
    drop(shared);
    drop(moved);
    registry.lock().unwrap().check_limits(low).unwrap();
}
#[test]
fn local_hot_growth_uses_the_same_physical_queue_and_retiring_books_block_shrink() {
    let registry = registry(SubscriberLimits {
        pending: 1,
        pending_total: 2,
        registrations: 1,
        ..Default::default()
    });
    let (rx, registration) = subscribe(&registry, Default::default());
    let original = registration.original.clone();
    send(&registry, 0);
    let first = rx.recv().unwrap();
    send(&registry, 0);
    assert!(rx.try_recv().is_err());
    let mut grown = registry.lock().unwrap().current_limits();
    grown.pending = 2;
    {
        let mut same = registry.lock().unwrap();
        same.check_limits(grown).unwrap();
        same.install_limits(grown);
        assert!(Arc::ptr_eq(&same[0].book, &original));
    }
    send(&registry, 0);
    let second = rx.recv().unwrap();
    drop(registration);
    let mut shrunk = grown;
    shrunk.pending = 1;
    assert_eq!(
        registry
            .lock()
            .unwrap()
            .check_limits(shrunk)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(first);
    registry.lock().unwrap().check_limits(shrunk).unwrap();
    drop(second);
    drop(original);
    drop(rx);
    assert_eq!(
        registry
            .lock()
            .unwrap()
            .root
            .registrations
            .load(Ordering::Acquire),
        0
    );
}
#[test]
fn aggregate_statistics_is_distinct_from_each_original_statistics_allowance() {
    let registry = registry(SubscriberLimits {
        stats: 8,
        stats_total: 12,
        ..Default::default()
    });
    let (one, first) = subscribe(&registry, Default::default());
    let (two, second) = subscribe(&registry, Default::default());
    send(&registry, 8);
    let held = one.recv().unwrap();
    assert!(two.try_recv().is_err());
    drop(first);
    let mut smaller = registry.lock().unwrap().current_limits();
    smaller.stats_total = 7;
    assert_eq!(
        registry
            .lock()
            .unwrap()
            .check_limits(smaller)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(held);
    registry.lock().unwrap().check_limits(smaller).unwrap();
    send(&registry, 4);
    let held = two.recv().unwrap();
    assert_eq!(second.original.stats.load(Ordering::Acquire), 4);
    drop(held);
    drop(second);
}
