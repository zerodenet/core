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
