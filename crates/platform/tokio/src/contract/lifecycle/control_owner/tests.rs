//! Concrete Box custody tests only, not native/Scope-close qualification.
use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use zero_traits::packet_metadata::PacketMetadataCounter;
struct Control {
    counter: Arc<PacketMetadataCounter>,
    expected: PacketMetadataLayout,
    dropped: Arc<AtomicBool>,
    cancels: Arc<AtomicUsize>,
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.cancels.fetch_add(1, Ordering::AcqRel);
    }
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(std::future::pending())
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        assert_eq!(
            self.counter.snapshot().occupied,
            self.expected,
            "the original Box is still in its destructor, so it remains charged"
        );
        self.dropped.store(true, Ordering::Release);
    }
}
struct Returned {
    counter: Arc<PacketMetadataCounter>,
    dropped: Arc<AtomicBool>,
    wakes: AtomicUsize,
}
impl std::task::Wake for Returned {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(self.dropped.load(Ordering::Acquire));
        assert_eq!(
            self.counter.snapshot().occupied,
            PacketMetadataCounter::owned_layout()
        );
        // Same-thread reentrant counter admission must not find a held gate.
        let empty = self
            .counter
            .reserve(
                PacketMetadataLayout::new(0, 0),
                std::task::Waker::noop().clone(),
            )
            .unwrap();
        drop(empty);
        self.wakes.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test]
async fn paid_original_box_outlives_cancelled_close_observer_and_refunds_after_deallocation() {
    let layout = PacketMetadataLayout::boxed::<Control>();
    let expected = PacketMetadataCounter::owned_layout()
        .checked_add(layout)
        .unwrap();
    let counter = PacketMetadataCounter::new_owned(expected).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let cancels = Arc::new(AtomicUsize::new(0));
    let returned = Arc::new(Returned {
        counter: counter.clone(),
        dropped: dropped.clone(),
        wakes: AtomicUsize::new(0),
    });
    let credit = counter
        .reserve(layout, std::task::Waker::from(returned.clone()))
        .unwrap();
    let mut original = BoxChannelControl::from_prepaid(
        Control {
            counter: counter.clone(),
            expected,
            dropped: dropped.clone(),
            cancels: cancels.clone(),
        },
        credit,
    )
    .ok()
    .unwrap();
    original.cancel(CloseReason::Shutdown);
    assert_eq!(cancels.load(Ordering::Acquire), 1);
    let mut closing = original.wait_closed(Instant::now() + std::time::Duration::from_secs(1));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(counter.snapshot().occupied, expected);
    assert!(!dropped.load(Ordering::Acquire));
    drop(original);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(returned.wakes.load(Ordering::Acquire), 1);
    assert_eq!(
        counter.snapshot().occupied,
        PacketMetadataCounter::owned_layout()
    );
}
#[test]
fn foreign_box_conversion_preserves_its_one_pointer_without_claiming_a_new_debit() {
    let baseline = PacketMetadataCounter::owned_layout();
    let counter = PacketMetadataCounter::new_owned(baseline).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let raw = Box::new(Control {
        counter: counter.clone(),
        expected: baseline,
        dropped: dropped.clone(),
        cancels: Arc::new(AtomicUsize::new(0)),
    });
    let pointer = raw.as_ref() as *const Control as *const ();
    let original: BoxChannelControl = raw.into();
    assert_eq!(
        original.as_ref() as *const dyn DynChannelControl as *const (),
        pointer
    );
    assert_eq!(counter.snapshot().occupied, baseline);
    drop(original);
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(counter.snapshot().occupied, baseline);
}
