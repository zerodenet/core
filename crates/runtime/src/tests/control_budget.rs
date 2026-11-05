use super::*;
fn limits() -> ControlBudgetLimits {
    ControlBudgetLimits {
        connections: 1,
        subscriptions: 1,
        queued_event_bytes: 4,
    }
}
#[test]
fn independent_actual_charges_are_lazy_and_remain_until_original_return() {
    let owner = ControlBudget::new(limits());
    assert_eq!(owner.usage(), ControlBudgetUsage::default());
    let socket = owner.reserve(ControlCharge::Connection).unwrap();
    assert!(owner.reserve(ControlCharge::Connection).is_err());
    let worker = owner.reserve(ControlCharge::Subscription).unwrap();
    let bytes = owner.reserve(ControlCharge::QueuedEventBytes(4)).unwrap();
    assert!(owner.reserve(ControlCharge::QueuedEventBytes(1)).is_err());
    let independent = ControlBudget::new(limits());
    let other = independent.reserve(ControlCharge::Connection).unwrap();
    owner.close_admission();
    assert!(owner.confirm_released().is_err());
    drop((socket, worker, bytes));
    owner.confirm_released().unwrap();
    assert_eq!(independent.usage().connections, 1);
    drop(other);
}
#[test]
fn candidate_shrink_fences_new_occupancy_and_grow_waits_for_commit() {
    let owner = ControlBudget::new(limits());
    let old = owner.reserve(ControlCharge::Connection).unwrap();
    let mut grow = limits();
    grow.connections = 2;
    let attempt = owner.prepare(grow).unwrap();
    assert!(owner.reserve(ControlCharge::Connection).is_err());
    attempt.retire().unwrap();
    assert_eq!(owner.usage().connections, 1);
    let attempt = owner.prepare(grow).unwrap();
    attempt.commit().unwrap();
    let second = owner.reserve(ControlCharge::Connection).unwrap();
    assert!(owner.prepare(limits()).is_err());
    drop(second);
    let attempt = owner.prepare(limits()).unwrap();
    assert!(owner.reserve(ControlCharge::Connection).is_err());
    attempt.retire().unwrap();
    let second = owner.reserve(ControlCharge::Connection).unwrap();
    drop((old, second));
}
