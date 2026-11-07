//! Same Root, original storage, synchronous reentry and observer cancellation.
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};
struct Owner {
    bytes: [u8; 3],
    root: Arc<PacketReturnTotals>,
    layout: PacketMetadataLayout,
    dropping: Arc<AtomicBool>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl AsMut<[u8]> for Owner {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
impl zero_traits::PacketStorage for Owner {}
impl Drop for Owner {
    fn drop(&mut self) {
        assert_eq!(
            self.root.driver_metadata().occupied,
            charged(self.layout),
            "the actual Box owner is still being destroyed"
        );
        self.dropping.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn packet_box_refunds_after_its_original_owner_and_canceled_observer_keeps_custody() {
    let layout = PacketMetadataLayout::boxed::<Owner>();
    let root = original(layout);
    let dropping = Arc::new(AtomicBool::new(false));
    let packet = zero_traits::PacketBuffer::from_prepaid_owner(
        Owner {
            bytes: [1, 2, 3],
            root: root.clone(),
            layout,
            dropping: dropping.clone(),
        },
        root.reserve_driver_metadata(layout).unwrap(),
    )
    .ok()
    .unwrap();
    let weak = Arc::downgrade(&root);
    let mut waiter = Box::pin(root.notified());
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(waiter.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(waiter); // canceled allocation waiter is not packet-return evidence
    assert_eq!(root.driver_metadata().occupied, charged(layout));
    assert_eq!(packet.as_ref(), &[1, 2, 3]);
    let returned = root.notified();
    tokio::pin!(returned);
    returned.as_mut().enable();
    drop(packet);
    assert!(dropping.load(Ordering::Acquire));
    tokio::time::timeout(std::time::Duration::from_secs(1), returned)
        .await
        .unwrap();
    assert_eq!(root.driver_metadata().occupied, owned());
    assert!(Weak::ptr_eq(&weak, &Arc::downgrade(&root)));
}
struct Reentrant {
    counter: Arc<zero_traits::packet_metadata::PacketMetadataCounter>,
    observed: AtomicBool,
}
impl std::task::Wake for Reentrant {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // Snapshot and a new reserve both enter the same tiny gate. A wake
        // before releasing the original gate would deadlock this same thread.
        let snapshot = self.counter.snapshot();
        assert_eq!(snapshot.occupied, owned());
        let empty = self
            .counter
            .reserve(
                PacketMetadataLayout::new(0, 0),
                std::task::Waker::noop().clone(),
            )
            .unwrap();
        drop(empty);
        self.observed.store(true, Ordering::Release);
    }
}
#[test]
fn pure_counter_refunds_both_units_before_synchronous_reentrant_wake() {
    let counter = zero_traits::packet_metadata::PacketMetadataCounter::new_owned(charged(
        PacketMetadataLayout::new(1, 13),
    ))
    .unwrap();
    let reentrant = Arc::new(Reentrant {
        counter: counter.clone(),
        observed: AtomicBool::new(false),
    });
    let credit = counter
        .reserve(
            PacketMetadataLayout::new(1, 13),
            std::task::Waker::from(reentrant.clone()),
        )
        .unwrap();
    assert_eq!(
        counter.snapshot().occupied,
        charged(PacketMetadataLayout::new(1, 13))
    );
    drop(credit);
    assert!(reentrant.observed.load(Ordering::Acquire));
}
