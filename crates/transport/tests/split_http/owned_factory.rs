//! Concrete callback/header custody, not IO authority or a closure receipt.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
struct Marker(Arc<AtomicUsize>);
impl Drop for Marker {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
#[test]
fn actual_factory_header_credit_survives_all_strong_clones_until_its_last_closed_weak() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let dropped = Arc::new(AtomicUsize::new(0));
    let marker = Marker(dropped.clone());
    let call_memory = memory.clone();
    let callback = move || {
        let _actual = &marker;
        OwnedXhttpFactoryCall::prepare(&call_memory, async {
            Err(io::Error::from(io::ErrorKind::Unsupported).into())
        })
    };
    let layout = OwnedXhttpCarrierFactory::metadata_layout_for(&callback).unwrap();
    let credit = memory.reserve(layout).unwrap();
    let factory = OwnedXhttpCarrierFactory::from_prepaid(callback, credit)
        .unwrap_or_else(|_| panic!("same prepaid layout"));
    let weak = match &factory {
        OwnedXhttpCarrierFactory::Paid(actual) => PacketMetadataArc::downgrade(&actual.0),
        _ => panic!("actual paid constructor"),
    };
    let clone = factory.clone();
    drop(factory);
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    assert_ne!(memory.snapshot().occupied, baseline);
    drop(clone);
    assert_eq!(
        dropped.load(Ordering::Acquire),
        1,
        "unique concrete Fn really destroyed"
    );
    assert!(weak.upgrade().is_none());
    assert_ne!(
        memory.snapshot().occupied,
        baseline,
        "actual Holder weak header remains paid"
    );
    drop(weak);
    assert_eq!(memory.snapshot().occupied, baseline);
}
#[test]
fn refused_factory_allocation_returns_the_original_callable_and_its_exact_credit() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let calls = Arc::new(AtomicUsize::new(0));
    let actual = calls.clone();
    let call_memory = memory.clone();
    let callback = move || {
        actual.fetch_add(1, Ordering::AcqRel);
        OwnedXhttpFactoryCall::prepare(&call_memory, async {
            Err(io::Error::from(io::ErrorKind::Unsupported).into())
        })
    };
    let credit = memory.reserve(PacketMetadataLayout::new(0, 0)).unwrap();
    let (source, original, credit) = OwnedXhttpCarrierFactory::from_prepaid(callback, credit)
        .err()
        .expect("actual insufficient layout");
    assert_eq!(source, PacketMemoryError::Capacity);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    assert_eq!(credit.layout(), PacketMetadataLayout::new(0, 0));
    drop(original);
    drop(credit);
    assert_eq!(memory.snapshot().occupied, baseline);
}

#[tokio::test]
async fn original_factory_future_box_is_destroyed_before_its_exact_metadata_refund() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let dropped = Arc::new(AtomicUsize::new(0));
    let marker = Marker(dropped.clone());
    let work = async move {
        let _original = marker;
        std::future::pending::<Result<XhttpCarrier, crate::RuntimeError>>().await
    };
    let mut call = OwnedXhttpFactoryCall::prepare(&memory, work);
    assert_ne!(memory.snapshot().occupied, baseline);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut call).poll(cx).is_pending())).await
    );
    drop(call);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(memory.snapshot().occupied, baseline);
}

#[tokio::test]
async fn actual_root_refusal_drops_unpolled_factory_work_without_allocating_its_box() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let snapshot = memory.snapshot();
    let remaining = PacketMetadataLayout::new(
        snapshot.limit.allocations() - snapshot.occupied.allocations(),
        snapshot.limit.bytes() - snapshot.occupied.bytes(),
    );
    let held = memory.reserve(remaining).unwrap();
    let full = memory.snapshot().occupied;
    let polled = Arc::new(AtomicUsize::new(0));
    let actual = polled.clone();
    let work = async move {
        actual.fetch_add(1, Ordering::AcqRel);
        Err(io::ErrorKind::Unsupported.into())
    };
    let call = OwnedXhttpFactoryCall::prepare(&memory, work);
    assert_eq!(memory.snapshot().occupied, full);
    let source = match call.await {
        Err(crate::RuntimeError::Io(source)) => source,
        _ => panic!("own metadata refusal"),
    };
    assert_eq!(source.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!(polled.load(Ordering::Acquire), 0);
    assert_eq!(memory.snapshot().occupied, full);
    drop(held);
    assert_eq!(memory.snapshot().occupied, snapshot.occupied);
}
