use super::*;
#[test]
fn actual_phase_arcs_survive_one_stable_receipt_and_later_phase_delivery() {
    // Pure delivery metadata: these facts confer no Native/Scope close proof.
    let delivery = Delivery::new();
    let record = Arc::new(RecordFailures::default());
    let pending = Arc::new(PendingFailures::default());
    record.add_pending(pending.clone());
    delivery.register(record.clone());
    let first = delivery.receive(
        &record,
        &record.final_control,
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
            .with_source(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
    );
    let first_actual = Arc::downgrade(record.final_control.0.get().unwrap());
    let retained = Arc::new(first);
    let receipt = retained.cleanup_failure_receipt().unwrap().clone();
    assert!(!receipt.is_acknowledged());
    // This is the same receiver retaining this exact outgoing Arc, not a
    // diagnostic-text match or an unrelated task/native acknowledgement.
    assert!(receipt.acknowledge_retained(&retained));
    assert!(delivery.acknowledged());
    let second = delivery.receive(
        &record,
        &pending.native,
        ChannelError::new(Stage::Close, Reason::Protocol, Impact::Component),
    );
    let second_actual = Arc::downgrade(pending.native.0.get().unwrap());
    assert!(second
        .cleanup_failure_receipt()
        .unwrap()
        .same_receipt(&receipt));
    assert!(!Arc::ptr_eq(
        record.final_control.0.get().unwrap(),
        pending.native.0.get().unwrap()
    ));
    assert!(delivery.failed());
    // The already-retained stable payload includes both actual phase cells.
    // It has no back edge to this delivery wrapper or any resource controller.
    drop(second);
    drop(pending);
    drop(record);
    drop(delivery);
    assert!(first_actual.upgrade().is_some());
    assert!(second_actual.upgrade().is_some());
    drop(retained);
    drop(receipt);
    assert!(first_actual.upgrade().is_none());
    assert!(second_actual.upgrade().is_none());
}
#[test]
fn removing_a_completed_faultless_record_does_not_remove_another_actual_fault() {
    let delivery = Delivery::new();
    let retired = Arc::new(RecordFailures::default());
    let failed = Arc::new(RecordFailures::default());
    delivery.register(retired.clone());
    delivery.register(failed.clone());
    let _ = delivery.receive(
        &failed,
        &failed.final_control,
        ChannelError::new(Stage::Close, Reason::Io, Impact::Component),
    );
    delivery.remove_empty(&retired);
    delivery.remove_empty(&failed);
    let records = delivery.payload.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert!(Arc::ptr_eq(&records[0], &failed));
}
