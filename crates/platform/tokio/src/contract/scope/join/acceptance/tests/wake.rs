use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Wake, Waker},
};

struct ReceiverWake {
    witness: TaskJoinWitness,
    wakes: AtomicUsize,
}
impl Wake for ReceiverWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.witness.0.joined.try_lock().is_ok(),
            "waking under the witness lock"
        );
        let scope = self.witness.0.owner.upgrade().unwrap();
        assert!(scope.state.try_lock().is_ok());
        assert!(scope.hosted_waker.try_lock().is_ok());
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn explicit_local_receiver_acceptance_wakes_the_real_host_after_releasing_all_locks() {
    let mut scope = hosted(1);
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async { Err(failure()) })
        .unwrap();
    let receipt = next(&mut scope).await;
    let joined = witness.wait().await;
    let receiver = Mutex::new(Some(joined.clone()));
    let probe = Arc::new(ReceiverWake {
        witness: witness.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    assert!(scope
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    let retained = receiver.lock().unwrap().as_ref().unwrap().clone();
    witness.acknowledge_hosted_failure(&retained).unwrap();
    assert!(receipt.local_failure_accepted());
    assert_eq!(probe.wakes.load(Ordering::SeqCst), 1);
    witness.acknowledge_hosted_failure(&joined).unwrap();
    assert_eq!(
        probe.wakes.load(Ordering::SeqCst),
        1,
        "same receipt is idempotent"
    );
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
