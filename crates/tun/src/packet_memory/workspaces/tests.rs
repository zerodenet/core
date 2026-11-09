use super::*;
#[test]
fn full_peer_workspace_union_is_prepaid_without_partial_debit() {
    let limit = PacketMemoryLimit {
        frames: 4,
        bytes: 1024,
    };
    let roots = Arc::new(PacketReturnTotals::new_with_transmit(
        limit,
        limit,
        PacketMemoryLimit {
            frames: 2,
            bytes: 296,
        },
    ));
    // Three actual control workspaces cannot fit: no early reader debit.
    assert!(matches!(
        roots.io_workspaces(&[128; 4], &[148; 3]),
        Err(PacketMemoryError::Capacity)
    ));
    assert_eq!(roots.native().occupied.frames, 0);
    assert_eq!(roots.transmit().occupied.frames, 0);
    let work = roots.io_workspaces(&[128; 4], &[148; 2]).unwrap();
    let held = work.reads[0].try_buffer().unwrap();
    for grant in work.iter() {
        grant.revoke();
    }
    assert_eq!(
        roots.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 128
        }
    );
    assert_eq!(roots.transmit().occupied.frames, 0);
    drop(work);
    assert_eq!(
        roots.native().occupied.bytes,
        128,
        "retired owner cannot refund held buffer"
    );
    drop(held);
    assert_eq!(roots.native().occupied.frames, 0);
}

#[test]
fn whole_actual_reader_control_set_rejects_metadata_before_any_partial_frame_debit() {
    let roots = Arc::new(PacketReturnTotals::default());
    roots.install_native(1, 64).unwrap();
    roots.install_transmit(2, 158).unwrap();
    let baseline = roots.driver_metadata().occupied;
    let layout = read::header::slot_layout()
        .unwrap()
        .repeated(2)
        .unwrap()
        .checked_add(PacketTransmitEnvelope::layout(1).unwrap())
        .unwrap();
    let exact = baseline.checked_add(layout).unwrap();
    let too_small = PacketMetadataLayout::new(exact.allocations(), exact.bytes() - 1);
    roots
        .install_original_limits(
            roots.returns().limit,
            roots.native().limit,
            roots.transmit().limit,
            too_small,
        )
        .unwrap();
    assert!(matches!(
        roots.io_workspaces(&[64], &[148]),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert_eq!(roots.native().occupied.frames, 0);
    assert_eq!(roots.transmit().occupied.frames, 0);
    assert_eq!(roots.driver_metadata().occupied, baseline);
    roots
        .install_original_limits(
            roots.returns().limit,
            roots.native().limit,
            roots.transmit().limit,
            exact,
        )
        .unwrap();
    let work = roots.io_workspaces(&[64], &[148]).unwrap();
    assert_eq!(roots.driver_metadata().occupied, exact);
    // The canonical peer-union control producer must have the same cold
    // reusable envelope as the single original control producer.
    for _ in 0..2 {
        let mut packet = work.transmit[0]
            .try_buffer()
            .unwrap()
            .into_packet(4)
            .unwrap_or_else(|_| panic!("same admitted control"));
        packet.prepare_transmit(1, 10).unwrap();
        let child = packet.try_derive_zeroed(&[10]).unwrap().pop().unwrap();
        assert_eq!(roots.driver_metadata().occupied, exact);
        assert_eq!(roots.transmit().occupied.frames, 2);
        drop(packet);
        drop(child);
        assert_eq!(roots.transmit().occupied.frames, 1);
        assert_eq!(roots.driver_metadata().occupied, exact);
    }
    for original in work.iter() {
        original.revoke();
    }
    drop(work);
    assert_eq!(roots.native().occupied.frames, 0);
    assert_eq!(roots.transmit().occupied.frames, 0);
    assert_eq!(roots.driver_metadata().occupied, baseline);
}
