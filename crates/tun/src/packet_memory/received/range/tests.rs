use super::*;
#[test]
fn refused_owned_range_returns_the_exact_original_without_new_native_or_metadata_debt() {
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let read = root.native_grant(64).unwrap();
    let view = read.memory_view().unwrap();
    let mut buffer = read.try_buffer().unwrap();
    buffer.as_mut_bytes()[..4].copy_from_slice(b"same");
    let original = buffer.into_packet(4).unwrap();
    let address = original.as_ptr();
    let baseline = root.driver_metadata().occupied;
    let (cause, original) = view.retain_received_range(original, 0..5).unwrap_err();
    assert_eq!(cause, PacketMemoryError::Capacity);
    assert_eq!(original.as_ptr(), address);
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        baseline,
    )
    .unwrap();
    let (cause, original) = view.retain_received_range(original, 0..4).unwrap_err();
    assert!(matches!(
        cause,
        PacketMemoryError::Busy | PacketMemoryError::Capacity
    ));
    assert_eq!(original.as_ptr(), address);
    assert_eq!(original.as_ref(), b"same");
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(original);
    assert!(read.try_buffer().is_ok());
    read.revoke();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
