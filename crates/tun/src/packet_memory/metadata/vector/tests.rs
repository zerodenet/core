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
#[test]
fn pending_table_slot_is_paid_before_row_install_and_empty_storage_is_not_a_refund() {
    let (root, read, view) = original(1, std::mem::size_of::<u64>());
    let mut table = PacketMetadataVec::<u64>::default();
    table.try_reserve_one(&view, 1).unwrap();
    assert!(table.is_empty());
    assert_eq!(table.capacity(), 1);
    let actual = root.driver_metadata().occupied;
    assert_eq!(
        actual,
        charged(PacketMetadataLayout::new(1, std::mem::size_of::<u64>()))
    );
    read.revoke();
    assert_eq!(root.driver_metadata().occupied, actual);
    assert_eq!(table.push_reserved(71), Ok(()));
    assert_eq!(table.push_reserved(72), Err(72));
    assert_eq!(&*table, &[71]);
    assert_eq!(table.remove(0), 71);
    assert_eq!(root.driver_metadata().occupied, actual);
    drop(table);
    assert_eq!(
        root.driver_metadata().occupied,
        charged(PacketMetadataLayout::new(0, 0))
    );
}

#[test]
fn neutral_host_table_keeps_exact_capacity_paid_across_mutation_retain_and_reuse() {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    let root = Arc::new(
        PacketReturnTotals::new_with_metadata(
            one,
            one,
            one,
            charged(PacketMetadataLayout::new(2, 3 * std::mem::size_of::<u64>())),
        )
        .unwrap(),
    );
    let memory = root.driver_metadata_view();
    let mut table = PacketMetadataVec::default();
    table.try_reserve_one_with_metadata(&memory, 2).unwrap();
    assert!(table.is_empty());
    assert_eq!(table.push_reserved(7u64), Ok(()));
    table.try_push_with_metadata(11, &memory, 2).unwrap();
    assert_eq!(table.capacity(), 2);
    let actual = memory.snapshot().occupied;
    assert_eq!(
        actual,
        charged(PacketMetadataLayout::array::<u64>(2).unwrap())
    );
    let (source, same) = table.try_push_with_metadata(13, &memory, 2).unwrap_err();
    assert_eq!(source, PacketMemoryError::Capacity);
    assert_eq!(same, 13);
    *table.iter_mut().find(|value| **value == 7).unwrap() = 17;
    assert_eq!(table[0], 17);
    *table.get_mut(0).unwrap() = 18;
    table.retain(|value| *value == 18);
    assert_eq!(&*table, &[18]);
    assert_eq!(memory.snapshot().occupied, actual);
    table.retain(|_| false);
    assert!(table.is_empty());
    assert_eq!(table.capacity(), 2);
    assert_eq!(memory.snapshot().occupied, actual);
    table.try_push_with_metadata(19, &memory, 2).unwrap();
    assert_eq!(memory.snapshot().occupied, actual);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        },
        "neutral table requires no native workspace or device"
    );
    drop(table);
    assert_eq!(
        memory.snapshot().occupied,
        charged(PacketMetadataLayout::new(0, 0))
    );
}
#[test]
fn neutral_growth_pays_old_and_new_and_old_packet_entry_keeps_revocation_fence() {
    let (root, read, packet) = original(1, 2 * std::mem::size_of::<u64>());
    let host = root.driver_metadata_view();
    let mut table = PacketMetadataVec::default();
    table.try_push_with_metadata(7u64, &host, 2).unwrap();
    let (source, same) = table.try_push_with_metadata(11, &host, 2).unwrap_err();
    assert_eq!(source, PacketMemoryError::Capacity);
    assert_eq!(same, 11);
    assert_eq!(&*table, &[7]);
    drop(table);
    read.revoke();
    let mut revoked = PacketMetadataVec::<u64>::default();
    assert_eq!(
        revoked.try_reserve_one(&packet, 1),
        Err(PacketMemoryError::Stopped)
    );
    assert_eq!(revoked.capacity(), 0);
    // Metadata-only Host authority is distinct from revoked native/read IO.
    revoked.try_reserve_one_with_metadata(&host, 1).unwrap();
    assert_eq!(revoked.push_reserved(same), Ok(()));
}
