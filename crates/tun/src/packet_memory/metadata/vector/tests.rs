use super::*;
fn charged(layout: PacketMetadataLayout) -> PacketMetadataLayout {
    zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
        .checked_add(layout)
        .unwrap()
}
fn original(
    allocations: usize,
    bytes: usize,
) -> (Arc<PacketReturnTotals>, NativeReadGrant, PacketMemoryView) {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    let root = Arc::new(
        PacketReturnTotals::new_with_metadata(
            one,
            one,
            one,
            zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
                .checked_add(PacketMetadataLayout::new(allocations, bytes))
                .unwrap(),
        )
        .unwrap(),
    );
    let read = root.native_grant(1).unwrap();
    let view = read.memory_view().unwrap();
    (root, read, view)
}
#[test]
fn table_growth_pays_old_and_new_capacity_and_removal_keeps_real_capacity_charged() {
    let (root, _read, view) = original(2, 3 * std::mem::size_of::<u64>());
    let mut table = PacketMetadataVec::default();
    table.try_push(7u64, &view, 2).unwrap();
    assert_eq!(table.capacity(), 1);
    table.try_push(11, &view, 2).unwrap();
    assert_eq!(&*table, &[7, 11]);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(PacketMetadataLayout::new(1, 2 * std::mem::size_of::<u64>()))
    );
    assert_eq!(table.remove(0), 7);
    assert_eq!(table.remove(0), 11);
    assert_eq!(
        root.driver_metadata().occupied.bytes(),
        charged(PacketMetadataLayout::new(0, 2 * std::mem::size_of::<u64>())).bytes()
    );
    drop(table);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(PacketMetadataLayout::new(0, 0))
    );
}
#[test]
fn impossible_self_growth_returns_the_new_row_and_keeps_existing_rows() {
    let (root, _read, view) = original(1, 2 * std::mem::size_of::<u64>());
    let mut table = PacketMetadataVec::default();
    table.try_push(7u64, &view, 2).unwrap();
    let (source, same) = table.try_push(11, &view, 2).unwrap_err();
    assert_eq!(source, PacketMemoryError::Capacity);
    assert_eq!(same, 11);
    assert_eq!(&*table, &[7]);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(PacketMetadataLayout::new(1, std::mem::size_of::<u64>()))
    );
}
#[test]
fn foreign_occupied_bytes_are_busy_and_a_real_release_allows_the_same_row() {
    let (root, _read, view) = original(2, 3 * std::mem::size_of::<u64>());
    let foreign = view
        .reserve_driver_metadata(PacketMetadataLayout::new(1, 3 * std::mem::size_of::<u64>()))
        .unwrap();
    let mut table = PacketMetadataVec::default();
    let (source, same) = table.try_push(7u64, &view, 1).unwrap_err();
    assert_eq!(source, PacketMemoryError::Busy);
    assert!(table.is_empty());
    drop(foreign);
    table.try_push(same, &view, 1).unwrap();
    assert_eq!(&*table, &[7]);
    drop(table);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(PacketMetadataLayout::new(0, 0))
    );
}
