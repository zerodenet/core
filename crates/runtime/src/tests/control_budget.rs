use super::*;
fn limits() -> ControlBudgetLimits {
    ControlBudgetLimits {
        connections: 1,
        subscriptions: 1,
        queued_event_bytes: 4,
        pipe_workspace_bytes: 2 * 1024 * 1024,
        messages: Default::default(),
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

#[test]
fn pipe_workspaces_share_one_total_and_candidate_without_borrowing_event_bytes() {
    let mut initial = limits();
    initial.pipe_workspace_bytes = 6;
    let owner = ControlBudget::new(initial);
    let original = owner.reserve(ControlCharge::PipeWorkspaceBytes(4)).unwrap();
    let event = owner.reserve(ControlCharge::QueuedEventBytes(4)).unwrap();
    assert_eq!(owner.usage().pipe_workspace_bytes, 4);
    assert!(owner.reserve(ControlCharge::PipeWorkspaceBytes(3)).is_err());
    let mut too_small = initial;
    too_small.pipe_workspace_bytes = 3;
    assert!(owner.prepare(too_small).is_err());
    assert_eq!(owner.usage().pipe_workspace_bytes, 4);
    let mut next = initial;
    next.pipe_workspace_bytes = 5;
    let pending = owner.prepare(next).unwrap();
    assert!(owner.reserve(ControlCharge::PipeWorkspaceBytes(2)).is_err());
    let tail = owner.reserve(ControlCharge::PipeWorkspaceBytes(1)).unwrap();
    pending.retire().unwrap();
    assert_eq!(owner.limits(), initial);
    drop(tail);
    assert_eq!(owner.usage().queued_event_bytes, 4);
    // These are only ledger permits. Concrete Windows original/Scope/ACK
    // cleanup is exercised by the actual provider regression before release.
    drop((original, event));
    owner.close_admission();
    owner.confirm_released().unwrap();
}

#[path = "control_budget/messages.rs"]
mod messages;

#[path = "control_budget/connection.rs"]
mod connection;
