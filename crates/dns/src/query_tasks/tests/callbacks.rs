use super::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::atomic::AtomicUsize,
    task::{Context, Wake, Waker},
};

struct BoundWake {
    context: OwnedDnsQueryTasks,
    calls: AtomicUsize,
    panic: Arc<usize>,
}
impl Wake for BoundWake {
    fn wake(self: Arc<Self>) {
        if self.calls.fetch_add(1, Ordering::AcqRel) != 0 {
            return;
        }
        let state = self
            .context
            .0
            .state
            .try_lock()
            .expect("query record lock held during original host callback");
        let registration = state.records[0]
            .registration
            .get()
            .unwrap()
            .as_ref()
            .expect("accepted original task ID/witness persisted before callback");
        assert_ne!(registration.id.0, 0);
        assert!(!registration.witness.is_joined());
        drop(state);
        assert!(
            self.context.ensure_open().is_ok(),
            "actual synchronous query owner is reentrant"
        );
        std::panic::panic_any(self.panic.clone());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_host_callback_panic_occurs_only_after_original_query_id_and_witness_are_retained() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let context = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let payload = Arc::new(8051usize);
    let callback = Arc::new(BoundWake {
        context: context.clone(),
        calls: AtomicUsize::new(0),
        panic: payload.clone(),
    });
    let waker = Waker::from(callback.clone());
    assert!(scope
        .poll_exit(&mut Context::from_waker(&waker))
        .is_pending());
    let (entered, entering) = oneshot::channel();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        context
            .spawn(
                async move {
                    entered
                        .send(OwnedDnsQueryTasks::current_witness().unwrap())
                        .unwrap();
                    std::future::pending::<()>().await;
                },
                CancellationToken::new(),
            )
            .unwrap();
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        panic.downcast_ref::<Arc<usize>>().unwrap(),
        &payload
    ));
    assert_eq!(callback.calls.load(Ordering::Acquire), 1);
    let (id, witness) = {
        let state = context.0.state.lock().unwrap();
        let bound = state.records[0]
            .registration
            .get()
            .unwrap()
            .as_ref()
            .unwrap();
        (bound.id, bound.witness.clone())
    };
    assert!(entering.await.unwrap().same_registration(&witness));
    assert!(!witness.is_joined());
    assert!(context.shutdown(Instant::now()).await.is_err());
    assert_eq!(context.0.state.lock().unwrap().records.len(), 1);
    let actual = timeout(Duration::from_secs(1), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.id(), id);
    assert!(actual.result().is_ok());
    assert_eq!(witness.try_joined().unwrap().id(), id);
    context
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    scope.acknowledge(&actual).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
