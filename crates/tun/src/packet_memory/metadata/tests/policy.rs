//! Target Source backing and published limits share the same occupied counter.
use super::*;
use std::task::Waker;
use zero_traits::packet_metadata::{PacketMetadataCounter, PacketMetadataLimitCandidate};
fn bytes(n: usize) -> PacketMetadataLayout {
    charged(PacketMetadataLayout::new(16, n))
}
fn headers() -> Arc<PacketMetadataCounter> {
    PacketMetadataCounter::new_owned(bytes(64 * 1024)).unwrap()
}
fn prepare(
    counter: &Arc<PacketMetadataCounter>,
    header: &Arc<PacketMetadataCounter>,
    target: PacketMetadataLayout,
) -> PacketMetadataLimitCandidate {
    let paid = header
        .reserve(
            PacketMetadataCounter::candidate_layout().unwrap(),
            Waker::noop().clone(),
        )
        .unwrap();
    counter.prepare_limit(target, paid).ok().unwrap()
}
#[test]
fn candidate_growth_is_only_for_actual_target_sources_until_metadata_publication() {
    let header = headers();
    let old = bytes(32);
    let target = bytes(128);
    let counter = PacketMetadataCounter::new_owned(old).unwrap();
    let original = counter
        .reserve(PacketMetadataLayout::new(1, 32), Waker::noop().clone())
        .unwrap();
    let candidate = prepare(&counter, &header, target);
    let issuer = candidate.issuer();
    let source_header = header
        .reserve(
            zero_traits::packet_metadata::PacketMetadataCandidateIssuer::source_layout().unwrap(),
            Waker::noop().clone(),
        )
        .unwrap();
    let source = issuer.prepare_source(source_header).ok().unwrap();
    assert!(source.same_counter(&counter));
    let backing = source
        .reserve(PacketMetadataLayout::new(1, 64), Waker::noop().clone())
        .unwrap();
    let occupied = counter.snapshot().occupied;
    assert_eq!(counter.snapshot().limit, old);
    assert_eq!(
        counter
            .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
            .err(),
        Some(PacketMemoryError::Busy)
    );
    candidate.seal().unwrap();
    candidate.validate_publication().unwrap();
    candidate.commit_validated_deferred();
    assert_eq!(
        counter.snapshot().limit,
        old,
        "deferred metadata has not released the original publication fence"
    );
    assert_eq!(counter.snapshot().occupied, occupied);
    assert!(counter
        .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
        .is_err());
    candidate.publish();
    assert_eq!(counter.snapshot().limit, target);
    let same_source = source
        .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
        .unwrap();
    drop(same_source);
    let published = counter
        .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
        .unwrap();
    drop(published);
    assert_eq!(counter.snapshot().occupied, occupied);
    drop(backing);
    drop(original);
    assert_eq!(counter.snapshot().occupied, owned());
    drop(source);
    drop(issuer);
    drop(candidate);
    assert_eq!(header.snapshot().occupied, owned());
}
#[test]
fn rollback_keeps_original_backing_and_its_credit_until_real_release_without_a_second_book() {
    let header = headers();
    let counter = PacketMetadataCounter::new_owned(bytes(32)).unwrap();
    let candidate = prepare(&counter, &header, bytes(128));
    let issuer = candidate.issuer();
    let source_header = header
        .reserve(
            zero_traits::packet_metadata::PacketMetadataCandidateIssuer::source_layout().unwrap(),
            Waker::noop().clone(),
        )
        .unwrap();
    let source = issuer.prepare_source(source_header).ok().unwrap();
    let backing = source
        .reserve(PacketMetadataLayout::new(1, 64), Waker::noop().clone())
        .unwrap();
    let held = counter.snapshot().occupied;
    candidate.rollback();
    assert_eq!(counter.snapshot().limit, bytes(32));
    assert_eq!(counter.snapshot().occupied, held);
    assert_eq!(
        source
            .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
            .err(),
        Some(PacketMemoryError::Paused)
    );
    let pending_header = header
        .reserve(
            PacketMetadataCounter::candidate_layout().unwrap(),
            Waker::noop().clone(),
        )
        .unwrap();
    let (reason, pending_header) = counter
        .prepare_limit(bytes(128), pending_header)
        .err()
        .unwrap();
    assert_eq!(reason, PacketMemoryError::Busy);
    drop(pending_header);
    source.revoke(); // Returns memory permission, never Native/Scope/ACK.
    assert_eq!(counter.snapshot().occupied, held);
    assert_eq!(
        counter
            .reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
            .err(),
        Some(PacketMemoryError::Busy)
    );
    let retry = prepare(&counter, &header, bytes(128));
    assert_eq!(
        counter.snapshot().occupied,
        held,
        "new accepted policy preserves the same original debt"
    );
    retry.rollback();
    drop(retry);
    drop(backing);
    assert_eq!(counter.snapshot().occupied, owned());
    let recovered = counter
        .reserve(PacketMetadataLayout::new(1, 32), Waker::noop().clone())
        .unwrap();
    drop(recovered);
    drop(source);
    drop(issuer);
    drop(candidate);
    assert_eq!(header.snapshot().occupied, owned());
}
#[test]
fn actual_held_backing_rejects_shrink_before_any_limit_or_original_is_changed() {
    let header = headers();
    let counter = PacketMetadataCounter::new_owned(bytes(128)).unwrap();
    let backing = counter
        .reserve(PacketMetadataLayout::new(1, 64), Waker::noop().clone())
        .unwrap();
    let before = counter.snapshot();
    let paid = header
        .reserve(
            PacketMetadataCounter::candidate_layout().unwrap(),
            Waker::noop().clone(),
        )
        .unwrap();
    let (reason, paid) = counter.prepare_limit(bytes(32), paid).err().unwrap();
    assert_eq!(reason, PacketMemoryError::Busy);
    assert_eq!(counter.snapshot(), before);
    drop(paid);
    drop(backing);
    let candidate = prepare(&counter, &header, bytes(32));
    candidate.seal().unwrap();
    candidate.validate_publication().unwrap();
    candidate.commit_validated_deferred();
    candidate.publish();
    assert_eq!(counter.snapshot().limit, bytes(32));
    assert_eq!(counter.snapshot().occupied, owned());
    drop(candidate);
    assert_eq!(header.snapshot().occupied, owned());
}

#[test]
fn rejection_drop_and_actual_credit_release_reenter_the_same_counter_outside_its_scalar_gate() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;
    struct Reenter {
        counter: Arc<PacketMetadataCounter>,
        observed: Arc<AtomicUsize>,
    }
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.counter.snapshot();
            self.observed.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for Reenter {
        fn drop(&mut self) {
            let _ = self.counter.snapshot();
            self.observed.fetch_add(1, Ordering::SeqCst);
        }
    }
    let counter = PacketMetadataCounter::new_owned(bytes(32)).unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let wake = || {
        Waker::from(Arc::new(Reenter {
            counter: counter.clone(),
            observed: observed.clone(),
        }))
    };
    // Counter Self bytes are also held on this lane. Choose a layout above
    // the actual whole ceiling, not merely above its remaining free bytes.
    let impossible = counter.snapshot().limit.bytes().checked_add(1).unwrap();
    assert_eq!(
        counter
            .reserve(PacketMetadataLayout::new(1, impossible), wake())
            .err(),
        Some(PacketMemoryError::Capacity)
    );
    assert_eq!(
        observed.load(Ordering::SeqCst),
        1,
        "the original rejected Waker was returned/dropped without a locked callback"
    );
    let credit = counter
        .reserve(PacketMetadataLayout::new(1, 32), wake())
        .unwrap();
    drop(credit);
    assert!(
        observed.load(Ordering::SeqCst) >= 3,
        "real refund wake and final original Waker destructor both reenter"
    );
    assert_eq!(counter.snapshot().occupied, owned());
}

#[test]
fn consumer_budget_is_the_same_original_lane_and_only_actual_host_candidate_can_release_growth() {
    use zero_traits::packet_metadata::PacketMetadataBudget;
    let header = headers();
    let counter = PacketMetadataCounter::new_owned(bytes(32)).unwrap();
    let view = PacketMetadataBudget::from_counter(counter.clone());
    let same = view.clone();
    assert!(view.same_budget(&same));
    let other =
        PacketMetadataBudget::from_counter(PacketMetadataCounter::new_owned(bytes(32)).unwrap());
    assert!(!view.same_budget(&other));
    let original = view
        .reserve(PacketMetadataLayout::new(1, 32), Waker::noop().clone())
        .unwrap();
    assert_eq!(view.snapshot(), counter.snapshot());
    let candidate = prepare(&counter, &header, bytes(128));
    let issuer = candidate.issuer();
    assert!(issuer.same_budget(&view));
    assert!(!issuer.same_budget(&other));
    assert_eq!(
        same.reserve(PacketMetadataLayout::new(1, 1), Waker::noop().clone())
            .err(),
        Some(PacketMemoryError::Busy)
    );
    candidate.seal().unwrap();
    candidate.validate_publication().unwrap();
    candidate.commit_validated_deferred();
    assert_eq!(view.snapshot().limit, bytes(32));
    candidate.publish();
    let fresh = same
        .reserve(PacketMetadataLayout::new(1, 64), Waker::noop().clone())
        .unwrap();
    assert_eq!(counter.snapshot().occupied, view.snapshot().occupied);
    drop(fresh);
    drop(original);
    drop(issuer);
    drop(candidate);
    assert_eq!(view.snapshot().occupied, owned());
}
