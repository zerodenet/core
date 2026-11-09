use super::*;
#[test]
fn actual_grant_array_clone_shares_paid_backing_and_last_held_buffer_keeps_only_its_original() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(2, 128).unwrap();
    let baseline = root.driver_metadata().occupied;
    let work = root.owned_io_workspaces(&[64; 2], &[]).unwrap();
    let paid = root.driver_metadata().occupied;
    let second = work.reads.clone();
    assert_eq!(work.reads.as_ptr(), second.as_ptr());
    assert_eq!(root.driver_metadata().occupied, paid);
    let held = second[0].try_buffer().unwrap();
    for read in work.iter() {
        read.revoke();
    }
    drop(work);
    assert_eq!(second.len(), 2);
    assert_eq!(root.native().occupied.frames, 1);
    drop(second);
    assert_eq!(root.native().occupied.frames, 1);
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    drop(held);
    assert_eq!(root.native().occupied.frames, 0);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn whole_variable_original_array_refusal_changes_no_frame_or_metadata_debit() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(2, 128).unwrap();
    let baseline = root.driver_metadata().occupied;
    let mandatory = read::header::slot_layout().unwrap().repeated(2).unwrap();
    // Enough for every actual Slot, but not its distinct returned owning array.
    let limit = baseline.checked_add(mandatory).unwrap();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        limit,
    )
    .unwrap();
    assert!(matches!(
        root.owned_io_workspaces(&[64; 2], &[]),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert_eq!(root.native().occupied.frames, 0);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
