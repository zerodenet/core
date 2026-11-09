use super::*;
fn zero() -> PacketMemoryLimit {
    PacketMemoryLimit {
        frames: 0,
        bytes: 0,
    }
}
#[test]
fn actual_return_array_stays_paid_after_every_element_is_removed_until_array_deallocation() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(3, 48).unwrap();
    let baseline = root.driver_metadata().occupied;
    let original = root.transmit_packet(16, 2, 32).unwrap();
    let mut batch = original.try_derive_owned(&[16, 16]).unwrap();
    let first = batch.pop().unwrap();
    let second = batch.pop().unwrap();
    drop(original);
    drop(first);
    drop(second);
    assert!(batch.is_empty());
    assert_eq!(root.transmit().occupied, zero());
    let retained = root.driver_metadata().occupied;
    assert!(retained.bytes() >= baseline.bytes() + 2 * std::mem::size_of::<PacketBuffer>());
    assert!(matches!(
        root.check_original_limits(
            root.returns().limit,
            root.native().limit,
            root.transmit().limit,
            baseline
        ),
        Err(PacketMemoryError::Busy)
    ));
    drop(batch);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn exhausted_owned_array_iterator_holds_actual_capacity_and_original_lease_until_drop() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let envelope = root.reserve_transmit_envelope(2, 32).unwrap();
    let batch = envelope.allocate_owned(&[16, 16]).unwrap();
    let mut actual = batch.into_iter();
    drop(envelope);
    drop(actual.next().unwrap());
    drop(actual.next_back().unwrap());
    assert_eq!(actual.len(), 0);
    assert_eq!(root.transmit().occupied, zero());
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    drop(actual);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn full_tx_budget_reuses_one_inline_result_without_any_return_array_allocation() {
    let root = Arc::new(PacketReturnTotals::default());
    root.install_transmit(2, 32).unwrap();
    let baseline = root.driver_metadata().occupied;
    let original = root.transmit_packet(16, 1, 16).unwrap();
    let paid = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        paid,
    )
    .unwrap();
    let mut batch = original.try_derive_owned(&[16]).unwrap();
    assert_eq!(root.driver_metadata().occupied, paid);
    let packet = batch.pop().unwrap();
    drop(original);
    drop(packet);
    assert!(batch.is_empty());
    assert_eq!(root.transmit().occupied, zero());
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn actual_array_layout_refusal_keeps_native_original_and_every_tx_counter_unchanged() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(16).unwrap();
    let mut original = read
        .try_buffer()
        .unwrap()
        .into_packet(4)
        .unwrap_or_else(|_| panic!("actual read"));
    original.copy_from_slice(b"same");
    let before = root.driver_metadata().occupied;
    let one = PacketTransmitEnvelope::layout(1).unwrap();
    let maximum = before.checked_add(one).unwrap();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        maximum,
    )
    .unwrap();
    let native = root.native();
    let tx = root.transmit();
    assert!(original.prepare_transmit(2, 32).is_err());
    assert_eq!(original.as_ref(), b"same");
    assert_eq!(root.native(), native);
    assert_eq!(root.transmit(), tx);
    assert_eq!(root.driver_metadata().occupied, before);
    drop(original);
    read.revoke();
}
struct ReentrantArray {
    envelope: PacketTransmitEnvelope,
    wakes: std::sync::atomic::AtomicUsize,
}
impl std::task::Wake for ReentrantArray {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.envelope.0.try_lock().is_ok(),
            "array refund under actual Envelope lock"
        );
        let root = self.envelope.0.lock().unwrap().remaining.root.clone();
        assert!(
            root.state.try_lock().is_ok(),
            "array refund under original Root lock"
        );
        self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[test]
fn actual_array_release_notifies_after_all_original_owner_locks_are_released() {
    use std::future::Future;
    let root = Arc::new(PacketReturnTotals::default());
    let envelope = root.reserve_transmit_envelope(2, 32).unwrap();
    let batch = envelope.allocate_owned(&[16, 16]).unwrap();
    let mut iterator = batch.into_iter();
    drop(iterator.next().unwrap());
    drop(iterator.next().unwrap());
    let observer = Arc::new(ReentrantArray {
        envelope,
        wakes: std::sync::atomic::AtomicUsize::new(0),
    });
    let waker = std::task::Waker::from(observer.clone());
    let mut changed = std::pin::pin!(root.notified());
    assert!(changed
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&waker))
        .is_pending());
    drop(iterator);
    assert!(observer.wakes.load(std::sync::atomic::Ordering::SeqCst) > 0);
}
