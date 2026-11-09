//! The actual callback is armed before rejection, and may reenter its owner.
use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::{Context, Poll, Wake, Waker},
};
struct Reenter {
    owner: Arc<Mutex<()>>,
    wakes: AtomicUsize,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.owner.try_lock().is_ok(),
            "external notification ran inside the actual enclosing owner lock"
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn rejected_icmp_observation_notifies_only_after_its_original_owner_unlocks() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let view = root.driver_metadata_view();
    let observation = IcmpAttemptObservation::from_prepaid(
        view.reserve(IcmpAttemptObservation::metadata_layout().unwrap())
            .unwrap(),
    )
    .unwrap_or_else(|_| panic!("actual observation prepaid"));
    assert!(matches!(
        observation.disposition(),
        IcmpAttemptDisposition::Pending
    ));
    let parent = Arc::new(Mutex::new(()));
    let reenter = Arc::new(Reenter {
        owner: parent.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(reenter.clone());
    let mut cx = Context::from_waker(&waker);
    let waiting = observation.wait_original();
    tokio::pin!(waiting);
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    let guard = observation.defer_notifications();
    let owner = parent.lock().unwrap();
    observation.reject_unstarted();
    assert_eq!(reenter.wakes.load(Ordering::SeqCst), 0);
    drop(owner);
    drop(guard);
    assert!(reenter.wakes.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(IcmpAttemptDisposition::KnownNoNative)
    ));
}
