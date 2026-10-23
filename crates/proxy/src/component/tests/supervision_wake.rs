use super::InlineSupervisor;
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};
#[derive(Default)]
struct DriverWake(AtomicUsize);
impl Wake for DriverWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn successive_host_scan_requests_rearm_the_driver_without_an_unrelated_wake() {
    let signal = tokio::sync::Notify::new();
    let driver = InlineSupervisor::default();
    let mut requests = 0;
    let work = poll_fn(|_| {
        requests += 1;
        if requests < 3 {
            signal.notify_waiters();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });
    let mut driving = Box::pin(driver.drive(&signal, |_| {}, work));
    let wake = Arc::new(DriverWake::default());
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    for _ in 0..2 {
        wake.0.store(0, Ordering::SeqCst);
        assert!(driving.as_mut().poll(&mut context).is_pending());
        assert!(
            wake.0.load(Ordering::SeqCst) > 0,
            "each real notification must schedule the driver, including after replacing its waiter"
        );
    }
    assert!(driving.as_mut().poll(&mut context).is_ready());
}
