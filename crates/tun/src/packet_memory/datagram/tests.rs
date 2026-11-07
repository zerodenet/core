use super::*;
fn root(tx: usize) -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 2,
            bytes: 256,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 128,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: tx,
        },
    ))
}
#[test]
fn control_capacity_rejection_does_not_precharge_native_reads_or_change_originals() {
    let root = root(147);
    assert!(matches!(
        root.datagram_workspaces([64; 2], Some(148)),
        Err(PacketMemoryError::Capacity)
    ));
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
#[test]
fn control_and_read_allocations_remain_charged_after_owner_revoke_until_memory_returns() {
    let root = root(148);
    let originals = root.datagram_workspaces([64; 2], Some(148)).unwrap();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 128
        }
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
    let read = originals[0].try_buffer().unwrap();
    let control = originals.control.as_ref().unwrap().try_buffer().unwrap();
    for grant in originals.iter() {
        grant.revoke();
    }
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
    assert!(matches!(
        root.datagram_workspaces([64; 2], Some(148)),
        Err(PacketMemoryError::Busy)
    ));
    drop(read);
    assert_eq!(root.native().occupied.frames, 0);
    assert!(matches!(
        root.datagram_workspaces([64; 2], Some(148)),
        Err(PacketMemoryError::Busy)
    ));
    assert_eq!(
        root.native().occupied.frames,
        0,
        "failed read+control attempt is atomic"
    );
    drop(control);
    let next = root.datagram_workspaces([64; 2], Some(148)).unwrap();
    for grant in next {
        grant.revoke();
    }
    assert_eq!(root.native().occupied.frames, 0);
    assert_eq!(root.transmit().occupied.frames, 0);
}
