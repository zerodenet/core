use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
    task::Wake,
};
struct Reenter {
    listener: Weak<Shared>,
    observed: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let shared = self.listener.upgrade().unwrap();
        let state = shared
            .state
            .try_lock()
            .expect("external accept wake runs after original listener unlock");
        let address = state.listener.as_ref().unwrap().local_addr().unwrap();
        assert!(address.ip().is_loopback());
        self.observed.store(true, Ordering::SeqCst);
    }
}
#[test]
fn actual_pending_accept_with_zero_coop_credit_flushes_reentrant_getter_after_native_unlock() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.handle().block_on(async {
        let (mut owner, mut view, address) = bound();
        let watch = Arc::new(Reenter {
            listener: Arc::downgrade(&view.shared),
            observed: AtomicBool::new(false),
        });
        let waker = Waker::from(watch.clone());
        let mut accept = Box::pin(view.accept_owned());
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
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
        assert!(!tokio::task::coop::has_budget_remaining());
        assert!(accept
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert!(
            watch.observed.load(Ordering::SeqCst),
            "actual native poll synchronously woke relay and reentered live getter"
        );
        drop(accept);
        assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        std::net::TcpListener::bind(address).unwrap();
    });
}
struct ReenterDrop {
    listener: Weak<Shared>,
    observed: Arc<AtomicBool>,
}
impl Wake for ReenterDrop {
    fn wake(self: Arc<Self>) {
        self.check_native_unlocked();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.check_native_unlocked();
    }
}
impl ReenterDrop {
    fn check_native_unlocked(&self) {
        let shared = self.listener.upgrade().unwrap();
        let state = shared
            .state
            .try_lock()
            .expect("external old accept waker Drop is outside native lock");
        assert!(state
            .listener
            .as_ref()
            .unwrap()
            .local_addr()
            .unwrap()
            .ip()
            .is_loopback());
    }
}
impl Drop for ReenterDrop {
    fn drop(&mut self) {
        self.check_native_unlocked();
        self.observed.store(true, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn actual_pending_accept_replaces_and_releases_external_waker_without_native_lock_or_cycle() {
    let (mut owner, mut view, _) = bound();
    let observed = Arc::new(AtomicBool::new(false));
    let watch = Arc::new(ReenterDrop {
        listener: Arc::downgrade(&view.shared),
        observed: observed.clone(),
    });
    let weak = Arc::downgrade(&watch);
    let old = Waker::from(watch);
    let mut accept = Box::pin(view.accept_owned());
    assert!(accept
        .as_mut()
        .poll(&mut Context::from_waker(&old))
        .is_pending());
    drop(old);
    assert!(accept
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert!(observed.load(Ordering::SeqCst));
    assert!(weak.upgrade().is_none());
    drop(accept);
    assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
}
