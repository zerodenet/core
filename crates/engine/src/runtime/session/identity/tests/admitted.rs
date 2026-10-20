use super::*;
use std::sync::atomic::AtomicUsize;

#[test]
fn successful_receipts_authenticate_the_real_issuer_even_when_two_engines_have_the_same_id() {
    let first = new_engine();
    let second = new_engine();
    let one = first.mint_session_identity().unwrap();
    let two = second.mint_session_identity().unwrap();
    assert_eq!(one.id(), two.id());
    let mut a = session(one.id());
    let mut b = session(two.id());
    let receipt = first
        .prepare_session_with_identity_admitted(&one, &mut a, "first")
        .unwrap();
    let other = second
        .prepare_session_with_identity_admitted(&two, &mut b, "second")
        .unwrap();
    first.check_admitted_session(&receipt, one.id()).unwrap();
    second.check_admitted_session(&other, two.id()).unwrap();
    refused(
        second.check_admitted_session(&receipt, two.id()),
        "different Engine",
    );
    refused(
        first.check_admitted_session(&other, one.id()),
        "different Engine",
    );
    refused(
        first.check_admitted_session(&receipt, SessionId(one.id().0 + 1)),
        "does not match",
    );
    first
        .track_session(a.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
    refused(receipt.check_active(), "finalized");
    other.check_active().unwrap();
    second
        .track_session(b.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}

#[test]
fn receipt_revocation_keeps_original_callback_and_full_auth_policy_without_retaining_device_leases()
{
    let engine = new_engine();
    let identity = engine.mint_session_identity().unwrap();
    let mut business = policy::principal(identity.id(), 1);
    let auth = business.auth.as_mut().unwrap();
    auth.up_bps = Some(123);
    auth.down_bps = Some(456);
    auth.device_limit = Some(1);
    auth.quota_remaining_bytes = Some(10);
    let expected = auth.clone();
    let strong = Arc::strong_count(&engine.next_session_id);
    let receipt = engine
        .prepare_session_with_identity_admitted(&identity, &mut business, "in")
        .unwrap();
    assert_eq!(Arc::strong_count(&engine.next_session_id), strong);
    let calls = Arc::new(AtomicUsize::new(0));
    let old = calls.clone();
    let mut handle = engine.track_session(business.id);
    assert!(handle.register_cancellation(move || {
        old.fetch_add(1, Ordering::SeqCst);
    }));
    let actual = calls.clone();
    let registry = Arc::downgrade(&engine.session_registry);
    receipt
        .on_revoked(move || {
            // Reentrant query proves registry removal/notification is outside its lock.
            let _ = registry.upgrade().unwrap().snapshot();
            actual.fetch_add(10, Ordering::SeqCst);
        })
        .unwrap();
    refused(
        receipt.on_revoked(|| panic!("must not replace original receiver")),
        "already registered",
    );
    engine.record_session_inbound_rx(business.id, 10);
    assert!(handle.is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 11);
    refused(receipt.check_active(), "cancelled");
    let finished = handle.finish(SessionOutcome::Cancelled).unwrap();
    assert_eq!(finished.auth, Some(expected));
    assert_eq!(finished.bytes_up, 10);
    assert_eq!(calls.load(Ordering::SeqCst), 11);
    // Keep both weak receipts alive while a different device takes the same slot.
    let _receipt_copy = receipt.clone();
    let mut replacement = policy::principal(SessionId(0), 2);
    replacement.auth.as_mut().unwrap().device_limit = Some(1);
    engine
        .prepare_session(&mut replacement, "replacement")
        .unwrap();
    engine
        .track_session(replacement.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
}

#[test]
fn finish_and_late_receiver_registration_notify_once_without_a_live_engine_owner() {
    let engine = new_engine();
    let mut business = session(SessionId(987));
    let receipt = engine
        .prepare_session_admitted(&mut business, "in")
        .unwrap();
    assert_ne!(receipt.id(), SessionId(987));
    let calls = Arc::new(AtomicUsize::new(0));
    let callback = calls.clone();
    engine
        .track_session(business.id)
        .finish(SessionOutcome::DirectRelayed)
        .unwrap();
    receipt
        .on_revoked(move || {
            callback.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let issuer = Arc::downgrade(&engine.next_session_id);
    drop(engine);
    assert!(issuer.upgrade().is_none());
    refused(receipt.check_active(), "finalized");
}

#[test]
fn registration_racing_finish_has_one_lock_free_notification_and_no_live_receipt() {
    for _ in 0..32 {
        let engine = new_engine();
        let mut business = session(SessionId(0));
        let receipt = engine
            .prepare_session_admitted(&mut business, "in")
            .unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let calls = Arc::new(AtomicUsize::new(0));
        let pending = receipt.clone();
        let ready = barrier.clone();
        let counted = calls.clone();
        let worker = std::thread::spawn(move || {
            ready.wait();
            pending
                .on_revoked(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
        });
        barrier.wait();
        engine
            .track_session(business.id)
            .finish(SessionOutcome::DirectRelayed)
            .unwrap();
        worker.join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        refused(receipt.check_active(), "finalized");
    }
}

#[test]
fn real_policy_refusal_never_returns_a_successful_receipt_or_reusable_ticket() {
    let engine = new_engine().with_flow_hook(policy::Hook {
        calls: Arc::new(AtomicUsize::new(0)),
        deny: Arc::new(AtomicBool::new(true)),
        ids: Arc::new(std::sync::Mutex::new(vec![])),
    });
    let identity = engine.mint_session_identity().unwrap();
    let mut business = session(identity.id());
    refused(
        engine
            .prepare_session_with_identity_admitted(&identity, &mut business, "in")
            .map(|_| ()),
        "actual hook rejection",
    );
    no_business(&engine);
    refused(
        engine
            .prepare_session_with_identity_admitted(&identity, &mut business, "in")
            .map(|_| ()),
        "already used",
    );
}
