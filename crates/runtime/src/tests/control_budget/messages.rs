use super::*;
#[tokio::test]
async fn original_request_response_capacity_and_pending_candidate_are_independent_and_notify_after_real_return(
) {
    let mut limits = limits();
    limits.messages.request_bytes_total = 8;
    limits.messages.response_bytes_total = 4;
    let owner = ControlBudget::new(limits);
    let mut request = owner.reserve(ControlCharge::RequestBytes(0)).unwrap();
    request.grow(8).unwrap();
    let buffer = vec![0_u8; 8];
    assert_eq!(buffer.capacity(), 8);
    let response = owner.reserve(ControlCharge::ResponseBytes(4)).unwrap();
    assert_eq!(
        (owner.usage().request_bytes, owner.usage().response_bytes),
        (8, 4)
    );
    assert!(request.grow(1).is_err());
    assert_eq!(owner.usage().request_bytes, 8);
    let mut target = limits;
    target.messages.request_bytes_total = 7;
    assert!(owner.prepare(target).is_err());
    target.messages.request_bytes_total = 8;
    target.messages.response_bytes_total = 3;
    assert!(owner.prepare(target).is_err());
    let pending = owner.prepare(limits).unwrap();
    assert!(owner.reserve(ControlCharge::RequestBytes(1)).is_err());
    pending.retire().unwrap();
    let changed = owner.capacity_changed();
    tokio::pin!(changed);
    changed.as_mut().enable();
    drop(buffer);
    assert_eq!(
        owner.usage().request_bytes,
        8,
        "buffer and accounting return are distinct actual actions"
    );
    drop(request);
    changed.await;
    assert_eq!(owner.usage().request_bytes, 0);
    assert_eq!(owner.usage().response_bytes, 4);
    drop(response);
    owner.close_admission();
    owner.confirm_released().unwrap();
}
#[tokio::test]
async fn global_command_charge_survives_actual_body_failure_until_join_and_result_reception() {
    let mut limits = limits();
    limits.messages.pending_commands = 1;
    let owner = ControlBudget::new(limits);
    let command = owner.reserve(ControlCharge::PendingCommand).unwrap();
    let original = Arc::new(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "actual original command fault",
    ));
    let fault = original.clone();
    let task = tokio::spawn(async move { Err::<(), _>(io::Error::new(fault.kind(), fault)) });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    assert_eq!(owner.usage().pending_commands, 1);
    assert!(owner.reserve(ControlCharge::PendingCommand).is_err());
    let returned = task.await.unwrap().unwrap_err();
    let retained = returned
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &original));
    assert_eq!(owner.usage().pending_commands, 1);
    drop(command);
    assert_eq!(owner.usage().pending_commands, 0);
    owner.close_admission();
    owner.confirm_released().unwrap();
}
