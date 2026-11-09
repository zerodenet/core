//! Actual same-root Envelope, read-slot and erased storage header custody.
use super::*;
#[test]
fn envelope_metadata_refusal_cannot_advance_either_tx_or_original_native_credit() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let metadata = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        metadata,
    )
    .unwrap();
    let tx = root.transmit();
    assert!(matches!(
        root.reserve_transmit_envelope(1, 8),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert!(matches!(
        root.transmit_workspace(148),
        Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
    ));
    assert_eq!(root.transmit(), tx);
    let mut original = read
        .try_buffer()
        .unwrap()
        .into_packet(4)
        .unwrap_or_else(|_| panic!("same original admitted read capacity"));
    original.copy_from_slice(b"same");
    assert!(original.prepare_transmit(0, 0).is_err());
    assert_eq!(original.as_ref(), b"same");
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(root.transmit(), tx);
    assert_eq!(root.driver_metadata().occupied, metadata);
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(original);
    assert!(read.try_buffer().is_ok());
    read.revoke();
}
#[test]
fn original_control_reuses_its_cold_paid_envelope_and_releases_derived_promises_only_after_all_actual_children_return(
) {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(2, 158).unwrap();
    let baseline = root.driver_metadata().occupied;
    let control = root.transmit_workspace(148).unwrap();
    let paid = root.driver_metadata().occupied;
    assert_eq!(
        paid,
        baseline
            .checked_add(PacketTransmitEnvelope::layout(1).unwrap())
            .unwrap()
            .checked_add(read::header::slot_layout().unwrap())
            .unwrap()
    );
    // No new metadata unit is available after the original was admitted.
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        paid,
    )
    .unwrap();
    for _ in 0..2 {
        let mut original = control
            .try_buffer()
            .unwrap()
            .into_packet(4)
            .unwrap_or_else(|_| panic!("same original admitted read capacity"));
        original.copy_from_slice(b"ctrl");
        original.prepare_transmit(1, 10).unwrap();
        let child = original.try_derive_zeroed(&[10]).unwrap().pop().unwrap();
        assert_eq!(
            root.transmit().occupied,
            PacketMemoryLimit {
                frames: 2,
                bytes: 158
            }
        );
        assert_eq!(root.driver_metadata().occupied, paid);
        drop(original);
        assert_eq!(
            root.transmit().occupied,
            PacketMemoryLimit {
                frames: 2,
                bytes: 158
            }
        );
        // The original returned, but the previous child's real data remains.
        drop(control.try_workspace().unwrap());
        assert_eq!(
            root.transmit().occupied,
            PacketMemoryLimit {
                frames: 2,
                bytes: 158
            }
        );
        drop(child);
        assert_eq!(
            root.transmit().occupied,
            PacketMemoryLimit {
                frames: 1,
                bytes: 148
            }
        );
        assert_eq!(root.driver_metadata().occupied, paid);
    }
    control.revoke();
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    // The revoked original grant still owns its paid actual slot header.
    drop(control);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn last_real_envelope_weak_header_keeps_its_metadata_credit_after_actual_data_returns() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let envelope = root.reserve_transmit_envelope(1, 8).unwrap();
    let weak = PacketMetadataArc::downgrade(&envelope.0);
    let packet = envelope.allocate(&[8]).unwrap().pop().unwrap();
    let paid = root.driver_metadata().occupied;
    drop(envelope);
    drop(packet);
    assert_eq!(root.transmit().occupied.frames, 0);
    assert!(weak.upgrade().is_none());
    assert!(root.driver_metadata().occupied.bytes() < paid.bytes());
    assert_eq!(
        root.driver_metadata().occupied,
        baseline
            .checked_add(PacketMetadataArc::<Mutex<Reservation>>::layout().unwrap())
            .unwrap()
    );
    assert!(matches!(
        root.check_original_limits(
            root.returns().limit,
            root.native().limit,
            root.transmit().limit,
            baseline
        ),
        Err(PacketMemoryError::Busy)
    ));
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, baseline);
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        baseline,
    )
    .unwrap();
}

struct ReentrantControl {
    read: NativeReadGrant,
    seen: std::sync::atomic::AtomicUsize,
}
impl std::task::Wake for ReentrantControl {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.read.0.state.try_lock().is_ok(),
            "actual promise refund/wake under original slot mutex"
        );
        self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[test]
fn unused_control_promise_refund_notifies_only_after_unlocking_the_actual_read_slot() {
    use std::future::Future;
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(2, 158).unwrap();
    let control = root.transmit_workspace(148).unwrap();
    let mut packet = control
        .try_buffer()
        .unwrap()
        .into_packet(4)
        .unwrap_or_else(|_| panic!("same admitted control capacity"));
    packet.prepare_transmit(1, 10).unwrap();
    let observe = Arc::new(ReentrantControl {
        read: control.clone(),
        seen: std::sync::atomic::AtomicUsize::new(0),
    });
    let waker = std::task::Waker::from(observe.clone());
    let mut changed = std::pin::pin!(root.notified());
    assert!(changed
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&waker))
        .is_pending());
    drop(packet); // actual input is gone; the derived promise was never used
    assert!(observe.seen.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 148
        }
    );
    assert!(control.try_buffer().is_ok());
    control.revoke();
}
