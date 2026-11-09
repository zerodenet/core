use super::*;
#[test]
fn receive_copy_pays_native_and_erased_storage_atomically_and_keeps_both_until_actual_drop() {
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 68,
        },
    ));
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let metadata = root.driver_metadata();
    let mut packet = view.copy_received_packet(b"four").unwrap();
    assert_eq!(packet.as_ref(), b"four");
    packet[0] = b'F';
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 68
        }
    );
    assert_eq!(
        root.driver_metadata().occupied,
        metadata
            .occupied
            .checked_add(PacketMetadataLayout::boxed::<Received>())
            .unwrap()
    );
    let charged = root.driver_metadata();
    assert!(matches!(
        view.copy_received_packet(b"x"),
        Err(PacketMemoryError::Busy)
    ));
    assert_eq!(root.driver_metadata(), charged);
    original.revoke();
    assert!(matches!(
        view.copy_received_packet(b"x"),
        Err(PacketMemoryError::Stopped)
    ));
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 4
        }
    );
    assert_eq!(packet.as_ref(), b"Four");
    drop(packet);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    assert_eq!(root.driver_metadata().occupied, metadata.occupied);
}
#[test]
fn metadata_refusal_cannot_partially_consume_native_capacity() {
    let root = Arc::new(PacketReturnTotals::default());
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let native = root.native();
    let metadata = root.driver_metadata();
    root.install_original_limits(
        root.returns().limit,
        native.limit,
        root.transmit().limit,
        metadata.occupied,
    )
    .unwrap();
    assert!(matches!(
        view.copy_received_packet(b"new"),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert_eq!(root.native(), native);
    assert_eq!(root.driver_metadata().occupied, metadata.occupied);
}
