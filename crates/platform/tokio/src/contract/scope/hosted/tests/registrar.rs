use super::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

type Installed = Arc<Mutex<Option<(TaskId, TaskJoinWitness)>>>;
struct InstalledWake {
    original: Weak<Shared>,
    installed: Installed,
    calls: AtomicUsize,
    payload: Arc<usize>,
}
impl Wake for InstalledWake {
    fn wake(self: Arc<Self>) {
        let original = self.original.upgrade().unwrap();
        assert!(original.state.try_lock().is_ok());
        assert!(original.hosted_waker.try_lock().is_ok());
        let installed = self.installed.try_lock().unwrap();
        let (id, witness) = installed
            .as_ref()
            .expect("registration was installed first");
        assert!(witness.scope_identity().matches(&TaskRegistrar(original)));
        assert!(id.0 > 0);
        drop(installed);
        let before = self.calls.fetch_add(1, Ordering::SeqCst);
        if before == 0 {
            std::panic::panic_any(self.payload.clone());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn registrar_defers_host_panic_until_original_id_and_witness_are_durably_installed() {
    let mut owner = scope(1);
    let registrar = owner.registrar();
    let installed: Installed = Arc::new(Mutex::new(None));
    let payload = Arc::new(71usize);
    let probe = Arc::new(InstalledWake {
        original: Arc::downgrade(&owner.0 .0),
        installed: installed.clone(),
        calls: AtomicUsize::new(0),
        payload: payload.clone(),
    });
    let waker = Waker::from(probe.clone());
    assert!(owner
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    let (release, waiting) = oneshot::channel();
    let callbacks = registrar.defer_callbacks();
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async move {
            waiting.await.unwrap();
            Err(ChannelError::new(Stage::Close, Reason::Io, Impact::Session))
        })
        .unwrap();
    *installed.lock().unwrap() = Some((id, witness.clone()));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    assert!(!witness.is_joined());
    let panic = catch_unwind(AssertUnwindSafe(|| drop(callbacks))).unwrap_err();
    assert!(Arc::ptr_eq(
        panic.downcast_ref::<Arc<usize>>().unwrap(),
        &payload
    ));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(counts(&owner), (1, 0, 1));
    release.send(()).unwrap();
    let receipt = next(&mut owner).await;
    assert_eq!(receipt.id(), id);
    let actual = witness.wait().await;
    assert!(witness.matches_joined(&actual));
    assert!(Arc::ptr_eq(
        &receipt.result().unwrap_err(),
        &actual.result().unwrap_err()
    ));
    assert!(!receipt.local_failure_accepted());
    witness.acknowledge_hosted_failure(&actual).unwrap();
    assert!(receipt.local_failure_accepted());
    owner.acknowledge(&receipt).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
