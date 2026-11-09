use super::*;
use std::future::Future;

#[test]
fn actual_read_box_is_prepaid_cold_and_keeps_slot_header_until_last_real_weak_drop() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, 64).unwrap();
    let baseline = root.driver_metadata().occupied;
    let read = root.native_grant(64).unwrap();
    let admitted = root.driver_metadata().occupied;
    assert_eq!(
        admitted,
        baseline.checked_add(slot_layout().unwrap()).unwrap()
    );
    let weak = PacketMetadataArc::downgrade(&read.0);
    // No late header allocation/admission is available at actual read time.
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        admitted,
    )
    .unwrap();
    let mut packet = read
        .try_buffer()
        .unwrap()
        .into_packet(4)
        .unwrap_or_else(|_| panic!("same admitted buffer"));
    packet.copy_from_slice(b"paid");
    assert_eq!(packet.as_ref(), b"paid");
    assert_eq!(root.driver_metadata().occupied, admitted);
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    read.revoke();
    drop(read);
    assert!(
        weak.upgrade().is_none(),
        "packet retains original weak/header credit, never the retired slot body"
    );
    drop(packet);
    assert!(weak.upgrade().is_none());
    assert_eq!(root.native().occupied.frames, 0);
    assert_eq!(
        root.driver_metadata().occupied,
        baseline
            .checked_add(PacketMetadataArc::<ReadSlot>::layout().unwrap())
            .unwrap()
    );
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, baseline);
}

#[test]
fn native_one_next_ack_header_is_paid_before_promotion_while_old_box_is_alive() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, 64).unwrap();
    root.install_transmit(1, 64).unwrap();
    let read = root.native_grant(64).unwrap();
    let cold = root.driver_metadata().occupied;
    let mut first = read
        .try_buffer()
        .unwrap()
        .into_packet(4)
        .unwrap_or_else(|_| panic!("same read"));
    first.copy_from_slice(b"data");
    // Refusal must preserve the SAME packet, lane and ready promise.
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        cold,
    )
    .unwrap();
    assert!(first.prepare_transmit(0, 0).is_err());
    assert_eq!(first.as_ref(), b"data");
    assert_eq!(root.native().occupied.frames, 1);
    assert_eq!(root.transmit().occupied.frames, 0);
    assert!(!read.workspace_returned());
    // Restore this explicit library root's original policy; no frame/cap is raised.
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        PacketMetadataLayout::new(16384, 16777216),
    )
    .unwrap();
    first.prepare_transmit(0, 0).unwrap();
    let promoted = root.driver_metadata().occupied;
    let second = read
        .try_buffer()
        .unwrap()
        .into_packet(3)
        .unwrap_or_else(|_| panic!("next actual ACK read under original Native1"));
    assert_eq!(root.native().occupied.frames, 1);
    assert_eq!(root.transmit().occupied.frames, 1);
    assert_eq!(root.driver_metadata().occupied, promoted);
    drop(first); // actual old Box/header returns before next original header reuse
    assert_eq!(root.transmit().occupied.frames, 0);
    assert_eq!(root.driver_metadata().occupied, cold);
    assert!(matches!(read.try_buffer(), Err(PacketMemoryError::Busy)));
    drop(second);
    assert!(read.workspace_returned());
    read.revoke();
}

struct Reentrant {
    read: NativeReadGrant,
    wakes: std::sync::atomic::AtomicUsize,
}
impl std::task::Wake for Reentrant {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.read.0.state.try_lock().is_ok(),
            "header callback under original slot lock"
        );
        self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[test]
fn actual_header_refund_and_read_notification_allow_synchronous_reentry() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_native(1, 64).unwrap();
    let read = root.native_grant(64).unwrap();
    let mut packet = read
        .try_buffer()
        .unwrap()
        .into_packet(1)
        .unwrap_or_else(|_| panic!("actual admitted read"));
    packet.prepare_transmit(0, 0).unwrap();
    let observe = Arc::new(Reentrant {
        read: read.clone(),
        wakes: 0.into(),
    });
    let wake = std::task::Waker::from(observe.clone());
    let mut changed = std::pin::pin!(root.notified());
    assert!(changed
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&wake))
        .is_pending());
    drop(packet);
    assert!(observe.wakes.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(read.workspace_returned());
    read.revoke();
}

struct HeaderDropProbe {
    root: Arc<PacketReturnTotals>,
    entered: Arc<std::sync::atomic::AtomicBool>,
    bytes: [u8; 1],
}
impl AsRef<[u8]> for HeaderDropProbe {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for HeaderDropProbe {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl zero_traits::PacketStorage for HeaderDropProbe {}
impl Drop for HeaderDropProbe {
    fn drop(&mut self) {
        assert!(
            matches!(
                self.root
                    .reserve_driver_metadata(PacketMetadataLayout::boxed::<Self>()),
                Err(PacketMemoryError::Busy | PacketMemoryError::Capacity)
            ),
            "erased storage's body is not its Box deallocation"
        );
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
#[test]
fn promise_can_drop_first_but_header_units_stay_until_after_actual_erased_box_free() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let layout = zero_traits::packet_metadata::PacketMetadataHeaderPromise::layout(
        PacketMetadataLayout::boxed::<HeaderDropProbe>(),
        1,
    )
    .unwrap();
    let paid = root.reserve_driver_metadata(layout).unwrap();
    let wake = std::task::Waker::from(root.clone());
    let promise = zero_traits::packet_metadata::PacketMetadataHeaderPromise::from_prepaid(
        PacketMetadataLayout::boxed::<HeaderDropProbe>(),
        1,
        1,
        paid,
        wake.clone(),
        wake,
    )
    .unwrap_or_else(|_| panic!("actual full header promise prepaid"));
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        root.driver_metadata().occupied,
    )
    .unwrap();
    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let packet = zero_traits::PacketBuffer::from_leased_owner(
        HeaderDropProbe {
            root: root.clone(),
            entered: entered.clone(),
            bytes: [0],
        },
        promise.try_lease(1).unwrap(),
    )
    .unwrap_or_else(|_| panic!("same actual storage layout"));
    drop(promise);
    assert_eq!(
        root.driver_metadata().occupied,
        baseline.checked_add(layout).unwrap()
    );
    drop(packet);
    assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(root.driver_metadata().occupied, baseline);
}
