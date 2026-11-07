use super::*;
use crate::configuration::ControlConnectionCharge;
#[test]
fn repeated_original_issuer_uses_one_local_book_and_pending_shrink_checks_held_rows() {
    let mut limits = limits();
    limits.messages.ipc_pending_commands = 2;
    limits.messages.ipc_event_frames = 2;
    limits.messages.grpc_streams = 2;
    let budget = ControlBudget::new(limits);
    let original = budget.reserve(ControlCharge::Connection).unwrap();
    let a = original.connection_budget().unwrap();
    let b = original.connection_budget().unwrap();
    let first = a.reserve(ControlConnectionCharge::PendingCommand).unwrap();
    let second = b.reserve(ControlConnectionCharge::PendingCommand).unwrap();
    assert!(a.reserve(ControlConnectionCharge::PendingCommand).is_err());
    assert_eq!(budget.usage().pending_commands, 2);
    let mut shrink = limits;
    shrink.messages.ipc_pending_commands = 1;
    assert!(budget.prepare(shrink).is_err());
    drop(first);
    let prepared = budget.prepare(shrink).unwrap();
    assert!(b.reserve(ControlConnectionCharge::PendingCommand).is_err());
    prepared.retire().unwrap();
    let event = a.reserve(ControlConnectionCharge::EventFrame).unwrap();
    let rpc = b.reserve(ControlConnectionCharge::GrpcStream).unwrap();
    assert_eq!(budget.usage().grpc_requests, 1);
    drop(original);
    assert!(a.reserve(ControlConnectionCharge::PendingCommand).is_err());
    assert_eq!(budget.usage().pending_commands, 1);
    drop(second);
    drop(event);
    drop(rpc);
    assert_eq!(budget.usage(), Default::default());
    budget.close_admission();
    budget.confirm_released().unwrap();
}

#[test]
fn sse_local_frames_are_distinct_from_ipc_and_shrink_sees_the_actual_held_write() {
    let mut limits = limits();
    limits.messages.sse_event_frames = 2;
    limits.messages.ipc_event_frames = 2;
    let budget = ControlBudget::new(limits);
    let connection = budget.reserve(ControlCharge::Connection).unwrap();
    let local = connection.connection_budget().unwrap();
    let first = local.reserve(ControlConnectionCharge::SseFrame).unwrap();
    let second = local.reserve(ControlConnectionCharge::SseFrame).unwrap();
    assert!(local.reserve(ControlConnectionCharge::SseFrame).is_err());
    let a = local.reserve(ControlConnectionCharge::EventFrame).unwrap();
    let b = local.reserve(ControlConnectionCharge::EventFrame).unwrap();
    let mut smaller = limits;
    smaller.messages.sse_event_frames = 1;
    assert!(budget.prepare(smaller).is_err());
    drop(second);
    let prepared = budget.prepare(smaller).unwrap();
    assert!(local.reserve(ControlConnectionCharge::SseFrame).is_err());
    prepared.retire().unwrap();
    smaller = limits;
    smaller.messages.ipc_event_frames = 1;
    assert!(budget.prepare(smaller).is_err());
    drop(b);
    budget.prepare(smaller).unwrap().retire().unwrap();
    drop(a);
    drop(first);
    drop(connection);
    budget.close_admission();
    budget.confirm_released().unwrap();
}
