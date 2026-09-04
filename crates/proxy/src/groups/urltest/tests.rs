use super::{CurrentProbeOperation, ProbeOperationState};

#[test]
fn automatic_cycle_queues_one_fresh_manual_operation() {
    let mut state = ProbeOperationState {
        current: Some(CurrentProbeOperation {
            operation_id: "scheduled-1".to_owned(),
            manual: false,
        }),
        pending_manual: None,
    };

    let first = state.request("manual-1".to_owned());
    let repeated = state.request("manual-2".to_owned());

    assert_eq!(first.operation_id, "manual-1");
    assert!(!first.coalesced);
    assert_eq!(repeated.operation_id, "manual-1");
    assert!(repeated.coalesced);
    assert_eq!(state.take_pending_manual().as_deref(), Some("manual-1"));
}

#[test]
fn overlapping_clicks_join_the_running_manual_operation() {
    let mut state = ProbeOperationState {
        current: Some(CurrentProbeOperation {
            operation_id: "manual-1".to_owned(),
            manual: true,
        }),
        pending_manual: None,
    };

    let ack = state.request("manual-2".to_owned());

    assert_eq!(ack.operation_id, "manual-1");
    assert!(ack.coalesced);
    assert!(state.pending_manual.is_none());
}
