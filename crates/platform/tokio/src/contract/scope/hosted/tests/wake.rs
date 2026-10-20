use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

struct ReentrantWake {
    owner: Weak<Shared>,
    wakes: AtomicUsize,
}
impl Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let owner = self.owner.upgrade().unwrap();
        assert!(
            owner.state.try_lock().is_ok(),
            "wake ran inside original State lock"
        );
        assert!(
            owner.hosted_waker.try_lock().is_ok(),
            "wake ran inside waker lock"
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
fn watch(owner: &HostedScope) -> (Arc<ReentrantWake>, Waker) {
    let probe = Arc::new(ReentrantWake {
        owner: Arc::downgrade(&owner.0 .0),
        wakes: AtomicUsize::new(0),
    });
    (probe.clone(), Waker::from(probe))
}

#[tokio::test]
async fn hosted_idle_poll_is_woken_by_real_registration_without_an_extra_driver_task() {
    let mut owner = scope(1);
    let (probe, waker) = watch(&owner);
    assert!(owner
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    let id = owner
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert!(probe.wakes.load(Ordering::SeqCst) > 0);
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    owner.acknowledge(&receipt).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn receipt_drop_and_ack_wake_outside_locks_after_releasing_the_offer() {
    let mut owner = scope(1);
    owner
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let receipt = next(&mut owner).await;
    let joined = receipt.joined().clone();
    let (probe, waker) = watch(&owner);
    assert!(owner
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    drop(receipt);
    assert!(probe.wakes.load(Ordering::SeqCst) > 0);
    let retry = next(&mut owner).await;
    assert!(joined.same_receipt(retry.joined()));
    let before = probe.wakes.load(Ordering::SeqCst);
    assert!(owner
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    owner.acknowledge(&retry).unwrap();
    assert!(probe.wakes.load(Ordering::SeqCst) > before);
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

struct ReentrantDrop {
    owner: Weak<Shared>,
    dropped: Arc<AtomicUsize>,
}
impl Wake for ReentrantDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        let owner = self.owner.upgrade().unwrap();
        assert!(owner.state.try_lock().is_ok());
        assert!(
            owner.hosted_waker.try_lock().is_ok(),
            "old waker dropped inside its mutex"
        );
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn replacing_a_host_waker_drops_its_last_strong_reference_outside_the_mutex() {
    let owner = scope(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(ReentrantDrop {
        owner: Arc::downgrade(&owner.0 .0),
        dropped: dropped.clone(),
    }));
    owner
        .0
         .0
        .register_hosted_waker(&Context::from_waker(&waker));
    drop(waker);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    owner
        .0
         .0
        .register_hosted_waker(&Context::from_waker(Waker::noop()));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
