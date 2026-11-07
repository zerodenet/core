use super::*;
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

#[test]
fn encoded_output_admission_is_atomic_and_cannot_wake_itself_on_partial_refunds() {
    let mut limits = limits();
    limits.messages.response_bytes_total = 20;
    let owner = ControlBudget::new(limits);
    let mut encoder = owner.reserve(ControlCharge::ResponseBytes(4)).unwrap();
    let blocker = owner.reserve(ControlCharge::ResponseBytes(10)).unwrap();
    let mut changed = pin!(owner.capacity_changed());
    changed.as_mut().enable();
    let before = owner.usage();
    assert!(encoder
        .reserve_encoded_response(2, ControlCharge::QueuedEventBytes(4), 5)
        .is_err());
    assert_eq!(
        owner.usage(),
        before,
        "failed composite admission keeps every actual counter unchanged"
    );
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        changed.as_mut().poll(&mut context).is_pending(),
        "no partial raw refund wakes its own full encoder"
    );
    drop(blocker);
    assert_eq!(changed.as_mut().poll(&mut context), Poll::Ready(()));
    let (raw, encoded) = encoder
        .reserve_encoded_response(2, ControlCharge::QueuedEventBytes(4), 5)
        .unwrap();
    assert_eq!(owner.usage().response_bytes, 11);
    assert_eq!(owner.usage().queued_event_bytes, 4);
    let mut smaller = limits;
    smaller.messages.response_bytes_total = 10;
    assert!(owner.prepare(smaller).is_err());
    drop((raw, encoded));
    assert_eq!(owner.usage().response_bytes, 6);
    smaller.messages.response_bytes_total = 15;
    let pending = owner.prepare(smaller).unwrap();
    let before = owner.usage();
    assert!(encoder
        .reserve_encoded_response(2, ControlCharge::ResponseBytes(4), 5)
        .is_err());
    assert_eq!(
        owner.usage(),
        before,
        "the same pending candidate fences all three storage obligations"
    );
    pending.retire().unwrap();
    let pair = encoder
        .reserve_encoded_response(2, ControlCharge::ResponseBytes(4), 5)
        .unwrap();
    assert_eq!(owner.usage().response_bytes, 17);
    drop((pair, encoder));
    owner.close_admission();
    owner.confirm_released().unwrap();
}
