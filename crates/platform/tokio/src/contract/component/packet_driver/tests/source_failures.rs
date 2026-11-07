//! Pure fault custody only; none of these metadata facts closes IO or a task.
use super::*;
fn child(
    reason: Reason,
) -> (
    Arc<faults::Delivery>,
    Arc<faults::RecordFailures>,
    ChannelError,
) {
    let producer = faults::Delivery::new();
    let phases = Arc::new(faults::RecordFailures::default());
    producer.register(phases.clone());
    let report = producer.receive(
        &phases,
        &phases.final_control,
        ChannelError::new(Stage::Close, reason, Impact::Component),
    );
    (producer, phases, report)
}
#[test]
fn a_different_child_cannot_borrow_an_already_accepted_forwarded_cell() {
    let source = SourceDelivery::new();
    let cell = source.prepare();
    let (first, _, first_report) = child(Reason::Protocol);
    let outer = Arc::new(source.receive(&cell, first_report));
    let receiver = crate::contract::lifecycle::ResourceCleanupFailures::default();
    assert!(receiver.try_receive(
        outer.clone(),
        crate::contract::lifecycle::CleanupFailurePhase::Driver
    ));
    assert!(source.accepted_or_empty());
    assert!(first.acknowledged());
    let (second, phases, second_report) = child(Reason::Io);
    let original = Arc::downgrade(phases.final_control.actual().unwrap());
    let rejected = source.receive(&cell, second_report);
    assert!(!source.owns(&rejected));
    assert!(
        !second.acknowledged(),
        "the old outer ACK does not accept this independent child"
    );
    drop(rejected);
    assert!(
        original.upgrade().is_some(),
        "the issuing actual phase still strongly owns its unaccepted cause"
    );
    let dedicated = source.prepare();
    let report = source.receive(&dedicated, second.report().unwrap());
    assert!(source.owns(&report));
    assert!(second.acknowledged());
    assert!(source.retains_child(&second.report().unwrap()));
    // This confirms delivery only. No logical/native/user/TaskJoined receipt
    // is manufactured by accepting the late child into the same pure payload.
    drop(report);
    drop(second);
    drop(phases);
    assert!(
        original.upgrade().is_some(),
        "the existing receiver's payload retains that exact late phase"
    );
    drop(cell);
    drop(dedicated);
    drop(source);
    drop(outer);
    assert!(original.upgrade().is_some());
    drop(receiver);
    assert!(original.upgrade().is_none());
}
