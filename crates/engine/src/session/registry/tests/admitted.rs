use super::*;
use zero_core::component::SessionId;

#[test]
fn removal_invalidates_before_notification_even_while_a_registry_reader_retains_the_entry() {
    let registry = Arc::new(SessionRegistry::default());
    let session = Session::new(
        17,
        Address::Domain("actual.test".into()),
        443,
        Network::Tcp,
        ProtocolType::new("fixture"),
    );
    let inserted = registry.insert(&session, "rule", None, None);
    let receipt = inserted
        .admission
        .receipt(SessionId(17), &Arc::new(AtomicU64::new(18)));
    let reader = registry.get(17).unwrap();
    let observed = Arc::new(AtomicBool::new(false));
    let actual = observed.clone();
    let weak = Arc::downgrade(&registry);
    let check = receipt.clone();
    receipt
        .on_revoked(move || {
            assert!(check.check_active().is_err());
            assert!(weak.upgrade().unwrap().snapshot().is_empty());
            actual.store(true, Ordering::Release);
        })
        .unwrap();
    registry
        .finish(17, SessionOutcome::DirectRelayed, None, None)
        .unwrap();
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(reader.id, 17);
    assert!(
        receipt.check_active().is_err(),
        "reader Arc is not admission liveness"
    );
}

#[test]
fn both_cancellation_receivers_run_once_outside_locks_and_original_panic_payload_wins() {
    for original_panics in [true, false] {
        let registry = Arc::new(SessionRegistry::default());
        let session = Session::new(
            17,
            Address::Domain("actual.test".into()),
            443,
            Network::Tcp,
            ProtocolType::new("fixture"),
        );
        let inserted = registry.insert(&session, "rule", None, None);
        let receipt = inserted.admission.receipt(
            zero_core::component::SessionId(17),
            &Arc::new(AtomicU64::new(18)),
        );
        let calls = Arc::new(AtomicU64::new(0));
        let original = Arc::new("original cancellation panic");
        let secondary = Arc::new("admitted receiver panic");
        let weak = Arc::downgrade(&registry);
        let count = calls.clone();
        let payload = original.clone();
        registry.register_cancellation(17, move || {
            let actual = weak.upgrade().unwrap();
            assert!(actual.is_cancelled(17));
            assert_eq!(actual.snapshot().len(), 1);
            count.fetch_add(1, Ordering::SeqCst);
            if original_panics {
                std::panic::panic_any(payload);
            }
        });
        let weak = Arc::downgrade(&registry);
        let count = calls.clone();
        let payload = secondary.clone();
        receipt
            .on_revoked(move || {
                let actual = weak.upgrade().unwrap();
                assert!(actual.is_cancelled(17));
                assert_eq!(actual.snapshot().len(), 1);
                count.fetch_add(10, Ordering::SeqCst);
                std::panic::panic_any(payload);
            })
            .unwrap();
        let panic =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| registry.cancel(17, "test")))
                .unwrap_err();
        let actual = panic.downcast::<Arc<&'static str>>().unwrap();
        let expected = if original_panics {
            &original
        } else {
            &secondary
        };
        assert!(Arc::ptr_eq(&actual, expected));
        assert_eq!(calls.load(Ordering::SeqCst), 11);
        assert!(registry.cancel(17, "again"));
        assert_eq!(calls.load(Ordering::SeqCst), 11);
        assert!(receipt.check_active().is_err());
        registry
            .finish(17, SessionOutcome::Cancelled, None, None)
            .unwrap();
    }
}
