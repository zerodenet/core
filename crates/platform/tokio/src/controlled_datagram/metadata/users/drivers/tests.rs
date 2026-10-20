use super::*;
use crate::contract::scope::TaskScope;
use std::{
    io,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::atomic::AtomicUsize,
    task::{Context, Wake, Waker},
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

type Owner = DriverOwner<u8, Arc<io::Error>>;
struct RegisteredWake {
    owner: WeakDrivers<u8, Arc<io::Error>>,
    calls: AtomicUsize,
    payload: Arc<usize>,
}
impl Wake for RegisteredWake {
    fn wake(self: Arc<Self>) {
        let owner = self.owner.upgrade().unwrap();
        let state = owner.shared.state.try_lock().unwrap();
        assert_eq!(state.pending, 0);
        assert_eq!(state.records.len(), 1);
        let (id, record) = state.records.first_key_value().unwrap();
        let registration = record.registration.try_lock().unwrap();
        let (actual, witness) = registration.as_ref().unwrap();
        assert_eq!(actual, id);
        assert!(witness.scope_identity().matches(&owner.shared.registrar));
        drop(registration);
        drop(state);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            std::panic::panic_any(self.payload.clone());
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_ledger_precedes_host_wake_panic_and_retains_original_join_until_real_reception() {
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        Some(NonZeroUsize::new(1).unwrap()),
    );
    scope.enable_registration().unwrap();
    let (owner, mut receiver): (Owner, _) =
        DriverOwner::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
    let payload = Arc::new(19usize);
    let probe = Arc::new(RegisteredWake {
        owner: owner.downgrade(),
        calls: AtomicUsize::new(0),
        payload: payload.clone(),
    });
    let waker = Waker::from(probe.clone());
    assert!(scope
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    let source = Arc::new(io::Error::from_raw_os_error(111));
    let original = source.clone();
    let (release, waiting) = oneshot::channel();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        owner.spawn(7, TaskClass::Auxiliary, |body| async move {
            waiting.await.unwrap();
            body.record(source.clone());
            Err(ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source))
        })
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        panic.downcast_ref::<Arc<usize>>().unwrap(),
        &payload
    ));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    let id = owner.tasks()[0];
    assert_eq!(owner.task_status().pending_registration, 0);
    assert_eq!(owner.task_status().unjoined, [id]);
    assert_eq!(owner.task_status().unreceived, [id]);
    release.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Auxiliary);
    assert!(!receipt.local_failure_accepted());
    let joined = owner
        .wait_joined(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &receipt.result().unwrap_err(),
        &joined[0].result().unwrap_err()
    ));
    assert_eq!(owner.task_status().unreceived, [id]);
    owner.reclaim_received();
    assert_eq!(owner.task_status().retained, 1);
    let delivery = receiver
        .receive(id, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        delivery.body.as_ref().unwrap().as_ref(),
        &original
    ));
    assert!(Arc::ptr_eq(
        &receipt.result().unwrap_err(),
        &delivery.joined.result().unwrap_err()
    ));
    assert!(receipt.local_failure_accepted());
    owner.reclaim_received();
    assert_eq!(owner.task_status().retained, 0);
    assert_eq!(owner.task_status().reclaimed, 1);
    assert!(scope.is_joined());
    assert!(!scope.is_drained(), "local receiver is not host ACK");
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
