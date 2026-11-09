use super::*;
fn original() -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::default())
}
fn smaller(root: &PacketReturnTotals) -> PacketMetadataLayout {
    let current = root.driver_metadata().limit;
    PacketMetadataLayout::new(current.allocations(), current.bytes() - 1)
}
#[test]
fn failed_shrink_preserves_actual_history_and_last_weak_header_without_sealing_old_policy() {
    let root = original();
    let baseline = root.driver_metadata().occupied;
    let layout = PacketMetadataArc::<[u8; 2048]>::layout().unwrap();
    let original = PacketMetadataArc::from_prepaid(
        [9_u8; 2048],
        root.reserve_driver_metadata(layout).unwrap(),
    )
    .ok()
    .unwrap();
    let weak = PacketMetadataArc::downgrade(&original);
    let next = PacketMetadataLayout::new(4096, baseline.bytes() + 1);
    assert!(matches!(
        root.pause_driver_metadata_for_limit(next),
        Err(PacketMemoryError::Busy)
    ));
    drop(original);
    assert!(weak.upgrade().is_none());
    assert!(matches!(
        root.pause_driver_metadata_for_limit(next),
        Err(PacketMemoryError::Busy)
    ));
    // Neither failed preflight changes policy nor stops actual admissions.
    assert!(root
        .reserve_driver_metadata(PacketMetadataLayout::boxed::<u8>())
        .is_ok());
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, baseline);
    let fence = root.pause_driver_metadata_for_limit(next).unwrap().unwrap();
    assert!(matches!(
        root.reserve_driver_metadata(layout),
        Err(PacketMemoryError::Paused)
    ));
    drop(fence);
    assert!(root.reserve_driver_metadata(layout).is_ok());
}
#[test]
fn successful_policy_install_finishes_old_fences_and_stale_drop_cannot_open_a_new_epoch() {
    let root = original();
    let view = root.driver_metadata_view();
    let next = smaller(&root);
    let first = root.pause_driver_metadata_for_limit(next).unwrap().unwrap();
    let second = root.pause_driver_metadata_for_limit(next).unwrap().unwrap();
    assert!(matches!(
        view.reserve(PacketMetadataLayout::boxed::<u8>()),
        Err(PacketMemoryError::Paused)
    ));
    drop(first);
    assert!(matches!(
        view.reserve(PacketMetadataLayout::boxed::<u8>()),
        Err(PacketMemoryError::Paused)
    ));
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        next,
    )
    .unwrap();
    assert!(
        view.reserve(PacketMetadataLayout::boxed::<u8>()).is_ok(),
        "new candidate can prepay before old publication fences drop"
    );
    let third = root
        .pause_driver_metadata_for_limit(smaller(&root))
        .unwrap()
        .unwrap();
    drop(second);
    assert!(matches!(
        view.reserve(PacketMetadataLayout::boxed::<u8>()),
        Err(PacketMemoryError::Paused)
    ));
    drop(third);
    assert!(view.reserve(PacketMetadataLayout::boxed::<u8>()).is_ok());
}
