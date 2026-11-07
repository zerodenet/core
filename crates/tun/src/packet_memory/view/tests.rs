use super::*;
#[test]
fn revoked_original_view_rejects_new_memory_and_keeps_held_transmit_charged() {
    let root = Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 128,
        },
    ));
    let original = root.native_grant(64).unwrap();
    let view = original.memory_view().unwrap();
    let packet = view.transmit_packet(64, 0, 0).unwrap();
    original.revoke();
    assert_eq!(
        view.transmit_packet(1, 0, 0).unwrap_err(),
        PacketMemoryError::Stopped
    );
    assert_eq!(root.transmit().occupied.bytes, 64);
    drop(view);
    drop(original);
    assert_eq!(root.transmit().occupied.bytes, 64);
    drop(packet);
    assert_eq!(root.transmit().occupied.bytes, 0);
}
