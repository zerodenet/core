use super::*;
use crate::controlled_carrier::CarrierOriginalTotals;
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
};

fn root(bytes: usize) -> Arc<CarrierOriginalTotals> {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 2,
        total_max_bytes: bytes,
    })
    .unwrap();
    root
}
struct Backing {
    bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
fn backing(credit: CarrierBufferCredit) -> Backing {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(credit.bytes()).unwrap();
    assert_eq!(bytes.capacity(), credit.bytes());
    Backing {
        bytes,
        _credit: credit,
    }
}
#[test]
fn actual_peak_and_transferred_backing_keep_same_charge_without_pinning_io_root() {
    let root = root(16);
    let book = root.buffer_budget();
    assert!(book.same_budget(&root.buffer_budget()));
    let mut peak = book.try_reserve(12).unwrap();
    let output = backing(peak.split_off(4).unwrap());
    let workspace = backing(peak);
    assert_eq!(output.bytes.capacity() + workspace.bytes.capacity(), 12);
    assert_eq!(book.used_bytes(), 12);
    assert_eq!(
        book.try_reserve(5).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(workspace);
    assert_eq!(book.used_bytes(), 4);
    let before = book.limits();
    assert_eq!(
        root.install_buffer_limits_after_retirement(CarrierBufferLimits {
            max_queue_packets: 1,
            total_max_bytes: 3,
        })
        .unwrap_err()
        .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(book.limits(), before);
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 1,
        total_max_bytes: 32,
    })
    .unwrap();
    assert_eq!(book.used_bytes(), 4);
    let weak = Arc::downgrade(&root);
    drop(root);
    assert!(
        weak.upgrade().is_none(),
        "pure byte credit has no IO/Scope/root back edge"
    );
    assert_eq!(book.used_bytes(), 4);
    drop(output);
    assert_eq!(book.used_bytes(), 0);
}
struct ReentrantWake {
    book: CarrierBufferBudget,
    root: std::sync::Weak<CarrierOriginalTotals>,
    calls: AtomicUsize,
}
impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.book.used_bytes();
        if let Some(root) = self.root.upgrade() {
            let _ = root.snapshot();
        }
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}
fn wake(root: &Arc<CarrierOriginalTotals>) -> Arc<ReentrantWake> {
    Arc::new(ReentrantWake {
        book: root.buffer_budget(),
        root: Arc::downgrade(root),
        calls: AtomicUsize::new(0),
    })
}
#[test]
fn actual_independent_waiters_need_complete_amount_and_refunds_wake_outside_both_locks() {
    let root = root(12);
    let book = root.buffer_budget();
    let mut held = book.try_reserve(12).unwrap();
    let four = held.split_off(4).unwrap();
    let two = held.split_off(2).unwrap();
    let first = wake(&root);
    let second = wake(&root);
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let mut first_cx = Context::from_waker(&first_waker);
    let mut second_cx = Context::from_waker(&second_waker);
    let mut six = pin!(book.reserve(6));
    let mut eight = pin!(book.reserve(8));
    assert!(six.as_mut().poll(&mut first_cx).is_pending());
    assert!(eight.as_mut().poll(&mut second_cx).is_pending());
    drop(two);
    assert_eq!(first.calls.load(Ordering::Acquire), 0);
    assert_eq!(second.calls.load(Ordering::Acquire), 0);
    drop(four);
    assert!(first.calls.load(Ordering::Acquire) > 0);
    assert!(
        second.calls.load(Ordering::Acquire) > 0,
        "a second actual writer's wake was not overwritten"
    );
    let Poll::Ready(Ok(six_credit)) = six.as_mut().poll(&mut first_cx) else {
        panic!("six bytes really fit")
    };
    assert!(eight.as_mut().poll(&mut second_cx).is_pending());
    let second_calls = second.calls.load(Ordering::Acquire);
    drop(held);
    assert_eq!(book.used_bytes(), 6);
    assert_eq!(
        second.calls.load(Ordering::Acquire),
        second_calls,
        "six available bytes cannot wake an eight-byte writer"
    );
    drop(six_credit);
    assert!(second.calls.load(Ordering::Acquire) > second_calls);
    let Poll::Ready(Ok(eight_credit)) = eight.as_mut().poll(&mut second_cx) else {
        panic!("same actual waiter gets its complete credit")
    };
    assert_eq!(book.used_bytes(), 8);
    drop(eight_credit);
    assert_eq!(book.used_bytes(), 0);
}
#[test]
fn oversize_never_waits_and_cancelled_waiters_own_no_byte_credit() {
    let root = root(8);
    let book = root.buffer_budget();
    let wake = wake(&root);
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut oversized = pin!(book.reserve(9));
    assert!(
        matches!(oversized.as_mut().poll(&mut cx), Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::InvalidInput)
    );
    let held = book.try_reserve(8).unwrap();
    {
        let mut abandoned = pin!(book.reserve(8));
        assert!(abandoned.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(book.used_bytes(), 8);
    drop(held);
    assert_eq!(book.used_bytes(), 0);
    let credit = book.try_reserve(8).unwrap();
    assert_eq!(book.used_bytes(), 8);
    drop(credit);
}
