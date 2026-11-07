use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
};
struct Reentrant {
    actual: Arc<Mutex<Option<zero_traits::PacketBuffer>>>,
    calls: AtomicUsize,
    view: PacketMemoryView,
}
impl Wake for Reentrant {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.actual.try_lock().is_ok(),
            "external wake must follow original owner unlock"
        );
        let allocation = self.view.transmit_packet(1, 0, 0).unwrap();
        // Real reentrant allocation succeeded because accounting was already
        // returned. Keep it in the original memory owner to avoid recursive
        // notifications from dropping an allocation in its own waker.
        *self.actual.lock().unwrap() = Some(allocation);
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}
#[test]
fn same_operation_unlocks_before_real_reentrant_wake_but_credit_is_immediately_reusable() {
    let root = Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let grant = root.native_grant(64).unwrap();
    let view = grant.memory_view().unwrap();
    let actual = Arc::new(Mutex::new(Some(view.transmit_packet(64, 0, 0).unwrap())));
    let callback = Arc::new(Reentrant {
        actual: actual.clone(),
        calls: AtomicUsize::new(0),
        view,
    });
    let waker = Waker::from(callback.clone());
    let mut cx = Context::from_waker(&waker);
    let notified = root.notified();
    tokio::pin!(notified);
    assert!(notified.as_mut().poll(&mut cx).is_pending());
    let delayed = grant.defer_notifications();
    {
        let mut owner = actual.lock().unwrap();
        drop(owner.take());
        assert_eq!(root.transmit().occupied.bytes, 0);
        assert_eq!(callback.calls.load(Ordering::Relaxed), 0);
        let next = grant.try_buffer().unwrap();
        drop(next);
        assert!(grant.workspace_returned());
    }
    drop(delayed);
    assert_eq!(callback.calls.load(Ordering::Relaxed), 1);
    assert!(matches!(notified.as_mut().poll(&mut cx), Poll::Ready(())));
    drop(actual.lock().unwrap().take());
}
