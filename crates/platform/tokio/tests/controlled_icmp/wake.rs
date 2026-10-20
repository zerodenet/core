use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
struct Reenter {
    entry: Weak<Entry>,
    close: AtomicBool,
    socket_observed: Mutex<Option<std::net::SocketAddr>>,
    receipt: Mutex<Option<IcmpCloseReceipt>>,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let entry = self.entry.upgrade().unwrap();
        let native = entry
            .native
            .try_lock()
            .expect("external wake must run after native unlock");
        if let Native::Live(socket) = &*native {
            *self.socket_observed.lock().unwrap() = Some(socket.local_addr().unwrap());
        }
        drop(native);
        if self.close.swap(false, Ordering::SeqCst) {
            *self.receipt.lock().unwrap() =
                Some(entry.close().expect("actual reentrant native close"));
        }
    }
}
#[test]
#[ignore = "actual native ICMP access required"]
fn real_pending_io_cooperative_wake_reenters_original_getter_and_explicit_close() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.handle().block_on(async {
        let _serial = LIVE.lock().await;
        let mut owner = owner(1);
        let mut view = owner.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
        let watch = Arc::new(Reenter {
            entry: Arc::downgrade(&view.entry),
            close: AtomicBool::new(true),
            socket_observed: Mutex::new(None),
            receipt: Mutex::new(None),
        });
        let waker = Waker::from(watch.clone());
        let mut bytes = [0; 128];
        let mut pending = Box::pin(view.recv_from(&mut bytes));
        assert!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending(),
            "actual native receive is suspended before exhausting cooperation"
        );
        // Handle::block_on gives this external executor a real coop budget but no
        // scheduler Context. Unlike #[tokio::test], exhausted poll_proceed calls
        // context::defer's immediate wake path, not the scheduler's deferred queue.
        // Repoll the same actual native IO while its ownership gate is held.
        let mut exhausted = false;
        for _ in 0..1024 {
            let mut budget = Box::pin(tokio::task::coop::consume_budget());
            if budget
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
            {
                exhausted = true;
                break;
            }
        }
        assert!(exhausted);
        assert!(
            !tokio::task::coop::has_budget_remaining(),
            "real coop credit is zero before native repoll"
        );
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        drop(pending);
        assert!(
            watch.socket_observed.lock().unwrap().is_some(),
            "callback inspected the real same live native socket"
        );
        assert!(watch
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .matches(&view));
        assert!(view.local_addr().is_err());
        assert_eq!(owner.take_report().confirmed_closed, 1);
        assert_eq!(owner.take_report().confirmed_closed, 0);
    });
}
struct ReenterDrop {
    entry: Weak<Entry>,
    receipt: Arc<Mutex<Option<IcmpCloseReceipt>>>,
}
impl Wake for ReenterDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for ReenterDrop {
    fn drop(&mut self) {
        let entry = self.entry.upgrade().unwrap();
        let native = entry
            .native
            .try_lock()
            .expect("external waker Drop must run after native unlock");
        let Native::Live(socket) = &*native else {
            panic!("same native socket still retained before callback")
        };
        assert!(socket.local_addr().unwrap().ip().is_loopback());
        drop(native);
        *self.receipt.lock().unwrap() = Some(entry.close().unwrap());
    }
}
#[tokio::test]
#[ignore = "actual native ICMP access required"]
async fn replacing_pending_io_waker_drops_external_target_outside_native_and_relay_locks() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let mut view = owner.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
    let receipt = Arc::new(Mutex::new(None));
    let watch = Arc::new(ReenterDrop {
        entry: Arc::downgrade(&view.entry),
        receipt: receipt.clone(),
    });
    let weak = Arc::downgrade(&watch);
    let old = Waker::from(watch);
    let mut bytes = [0; 128];
    let mut pending = Box::pin(view.recv_from(&mut bytes));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(&old))
        .is_pending());
    drop(old);
    assert!(matches!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        std::task::Poll::Ready(Err(_))
    ));
    drop(pending);
    assert!(
        weak.upgrade().is_none(),
        "native relay holds no caller target strongly"
    );
    assert!(receipt.lock().unwrap().as_ref().unwrap().matches(&view));
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
