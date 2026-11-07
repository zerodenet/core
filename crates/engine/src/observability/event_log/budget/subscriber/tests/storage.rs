use super::*;
#[test]
fn filter_is_moved_once_and_unregister_without_new_events_frees_exact_registration() {
    let registry = registry(Default::default());
    let mut label = String::with_capacity(111);
    label.push_str("same.event");
    let address = label.as_ptr();
    let mut filter = EventFilter::default();
    filter.event_types = Vec::with_capacity(4);
    filter.event_types.push(label);
    let expected = 4 * std::mem::size_of::<String>() + 111;
    assert_eq!(
        super::super::storage::filter_bytes(&filter).unwrap(),
        expected
    );
    let (rx, registration) = subscribe(&registry, filter);
    let root = registry.lock().unwrap().root.clone();
    assert_eq!(registration.filter().event_types[0].as_ptr(), address);
    assert_eq!(
        registry.lock().unwrap()[0].book.filter.event_types[0].as_ptr(),
        address
    );
    let paid = root.storage.load(Ordering::Acquire);
    assert!(paid >= expected + super::super::storage::queue_array_bytes(1024).unwrap());
    drop(rx);
    drop(registration);
    let mut same = registry.lock().unwrap();
    assert!(same.is_empty());
    assert_eq!(root.registrations.load(Ordering::Acquire), 0);
    // Weak allocation/vector capacity still really exists and remains paid.
    assert!(root.storage.load(Ordering::Acquire) > 0);
    let limits = same.current_limits();
    same.check_limits(limits).unwrap(); // Actually destroys the tombstone/buffers.
    assert_eq!(root.storage.load(Ordering::Acquire), 0);
}
#[test]
fn low_storage_and_filter_limits_refuse_before_constructing_a_physical_queue() {
    let registry = registry(SubscriberLimits {
        pending: 1,
        storage_total: 1,
        ..Default::default()
    });
    let root = registry.lock().unwrap().root.clone();
    for _ in 0..32 {
        assert_eq!(
            PreparedSubscriber::prepare(&registry, Default::default())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(registry.lock().unwrap().is_empty());
        assert_eq!(root.storage.load(Ordering::Acquire), 0);
        assert_eq!(root.registrations.load(Ordering::Acquire), 0);
    }
    let mut limits = SubscriberLimits::default();
    limits.filter = 1;
    registry.lock().unwrap().install_limits(limits);
    let mut filter = EventFilter::default();
    filter.event_types.push("same.event".into());
    assert_eq!(
        PreparedSubscriber::prepare(&registry, filter)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(root.storage.load(Ordering::Acquire), 0);
    assert_eq!(
        super::super::storage::vector_bytes::<String>(usize::MAX)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}
#[test]
fn prepared_vacancy_survives_another_receiver_drop_and_real_failed_install_cleanup() {
    let registry = registry(Default::default());
    let (rx, registration) = subscribe(&registry, Default::default());
    let prepared = PreparedSubscriber::prepare(&registry, Default::default()).unwrap();
    drop(rx);
    drop(registration);
    let (tx, rx) = mpsc::sync_channel(1024);
    let next = prepared.install(tx).unwrap(); // No unprepaid Vec expansion.
    assert_eq!(registry.lock().unwrap().len(), 1);
    drop(prepared);
    drop(rx);
    drop(next);
    let mut same = registry.lock().unwrap();
    let limits = same.current_limits();
    same.check_limits(limits).unwrap();
    assert_eq!(same.root.storage.load(Ordering::Acquire), 0);
}

#[test]
fn rejected_install_keeps_the_original_array_charge_until_receiver_is_released() {
    let registry = registry(Default::default());
    let root = registry.lock().unwrap().root.clone();
    let prepared = PreparedSubscriber::prepare(&registry, Default::default()).unwrap();
    let (tx, rx) = mpsc::sync_channel(1024);
    drop(registry);
    assert_eq!(
        prepared.install(tx).err().unwrap().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(
        root.storage.load(Ordering::Acquire)
            >= super::super::storage::queue_array_bytes(1024).unwrap()
    );
    drop(rx); // The real physical array precedes prepared's storage refund.
    drop(prepared);
    assert_eq!(root.storage.load(Ordering::Acquire), 0);
    assert_eq!(root.registrations.load(Ordering::Acquire), 0);
}
