use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};
use zero_platform_tokio::contract::CancellationToken;

struct PanicWaker {
    calls: Arc<AtomicUsize>,
    message: &'static str,
}

impl Wake for PanicWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(self.message);
    }
}

struct CountWaker(Arc<AtomicUsize>);

impl Wake for CountWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn panicking_child_waker_still_cancels_and_wakes_all_siblings_without_poisoning_tree() {
    let root = CancellationToken::new();
    let child_a = root.child_token();
    let child_b = root.child_token();
    let grandchild = child_a.child_token();

    let panic_calls = Arc::new(AtomicUsize::new(0));
    let wake_count_b = Arc::new(AtomicUsize::new(0));
    let wake_count_root = Arc::new(AtomicUsize::new(0));

    let panic_waker = Waker::from(Arc::new(PanicWaker {
        calls: panic_calls.clone(),
        message: "first child waker panic",
    }));
    let ok_waker_b = Waker::from(Arc::new(CountWaker(wake_count_b.clone())));
    let ok_waker_root = Waker::from(Arc::new(CountWaker(wake_count_root.clone())));

    let mut fut_grandchild = Box::pin(grandchild.cancelled_owned());
    let mut fut_b = Box::pin(child_b.cancelled());
    let mut fut_root = Box::pin(root.cancelled());

    assert_eq!(
        Pin::as_mut(&mut fut_grandchild).poll(&mut Context::from_waker(&panic_waker)),
        Poll::Pending
    );
    assert_eq!(
        Pin::as_mut(&mut fut_b).poll(&mut Context::from_waker(&ok_waker_b)),
        Poll::Pending
    );
    assert_eq!(
        Pin::as_mut(&mut fut_root).poll(&mut Context::from_waker(&ok_waker_root)),
        Poll::Pending
    );

    let unwind = std::panic::catch_unwind(|| root.cancel()).expect_err("waker panic must resume");
    assert_eq!(
        unwind.downcast_ref::<&str>().copied(),
        Some("first child waker panic")
    );

    assert!(root.is_cancelled());
    assert!(child_a.is_cancelled());
    assert!(child_b.is_cancelled());
    assert_eq!(panic_calls.load(Ordering::SeqCst), 1);
    assert_eq!(wake_count_b.load(Ordering::SeqCst), 1);
    assert_eq!(wake_count_root.load(Ordering::SeqCst), 1);
    assert!(root.child_token().is_cancelled());

    assert_eq!(
        Pin::as_mut(&mut fut_b).poll(&mut Context::from_waker(&ok_waker_b)),
        Poll::Ready(())
    );
    assert_eq!(
        Pin::as_mut(&mut fut_root).poll(&mut Context::from_waker(&ok_waker_root)),
        Poll::Ready(())
    );
}
