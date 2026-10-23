use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Poll, Wake},
};

#[tokio::test]
async fn weak_identity_and_callback_guard_never_keep_a_scope_or_create_admission() {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let weak = owner.registrar().downgrade();
    let original = owner.registrar().scope_identity();
    let identity = weak.scope_identity();
    let foreign = TaskScope::suspended_hosted(Handle::current(), None);
    assert!(identity.same_scope(&original));
    assert!(!identity.same_scope(&foreign.registrar().scope_identity()));
    assert_eq!(weak.0.strong_count(), 1);
    let callbacks = weak.defer_callbacks().unwrap();
    assert_eq!(
        weak.0.strong_count(),
        1,
        "guard owns only original relay metadata"
    );
    assert!(!identity.resource_open());
    assert_eq!(
        weak.spawn(TaskClass::Auxiliary, async {
            panic!("suspended weak body polled")
        }),
        Err(SpawnError::NotStarted)
    );
    owner.enable_registration().unwrap();
    assert!(identity.resource_open());
    owner.begin_shutdown();
    assert!(!identity.resource_open());
    assert!(
        weak.defer_callbacks().is_some(),
        "live closed metadata is not spawn permission"
    );
    assert_eq!(
        weak.spawn(TaskClass::Auxiliary, async {
            panic!("closed weak body polled")
        }),
        Err(SpawnError::Closing)
    );
    drop(owner);
    assert!(
        weak.0.upgrade().is_none(),
        "live guard cannot retain the original Scope"
    );
    assert!(weak.defer_callbacks().is_none());
    assert!(
        identity.same_scope(&weak.scope_identity()),
        "expired weak identity stays same metadata"
    );
    assert!(!identity.resource_open());
    drop(callbacks);
}

struct ParentWake {
    parent: Arc<Mutex<()>>,
    weak: WeakTaskRegistrar,
    calls: AtomicUsize,
}
impl Wake for ParentWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        drop(
            self.parent
                .try_lock()
                .expect("same original callback ran under the parent lock"),
        );
        assert_eq!(
            self.weak.spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity),
            "actual native exit remains charged during original notification delivery"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn weak_deferral_delivers_same_native_failure_after_parent_unlock_then_releases_exact_credit()
{
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    owner.enable_registration().unwrap();
    let weak = owner.registrar().downgrade();
    let (mut io, mut remote) = tokio::io::duplex(1);
    let (entered, pending) = oneshot::channel();
    let (id, witness) = weak
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let mut entered = Some(entered);
            let read = io.read_u8();
            tokio::pin!(read);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .unwrap();
            Err(
                ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(
                    std::io::Error::other("same actual weak-registered task failure"),
                ),
            )
        })
        .unwrap();
    pending.await.unwrap(); // The actual task is suspended in its original read.
    let parent = Arc::new(Mutex::new(()));
    let callback = Arc::new(ParentWake {
        parent: parent.clone(),
        weak: weak.clone(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(callback.clone());
    let mut waiting = Box::pin(witness.wait());
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    remote.write_u8(1).await.unwrap();
    let (receipt, local) = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            // Establish parent deferral before taking the caller's lock. A
            // final temporary Scope upgrade can never be released inside it.
            let callbacks = weak.defer_callbacks().unwrap();
            let joined = {
                let locked = parent.lock().unwrap();
                let joined = match owner.poll_exit(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(Some(receipt)) => {
                        let local = witness
                            .try_joined()
                            .expect("actual native join persisted before notification");
                        assert_eq!(local.id(), id);
                        assert_eq!(local.class(), TaskClass::Auxiliary);
                        assert!(Arc::ptr_eq(
                            &local.result().unwrap_err(),
                            &receipt.result().unwrap_err()
                        ));
                        // Persist the exact local result before either ACK.
                        let retained = (receipt, local);
                        witness.acknowledge_hosted_failure(&retained.1).unwrap();
                        owner.acknowledge(&retained.0).unwrap();
                        Some(retained)
                    }
                    Poll::Pending => None,
                    Poll::Ready(None) => panic!("actual registration disappeared"),
                };
                assert_eq!(callback.calls.load(Ordering::SeqCst), 0);
                assert_eq!(weak.0.strong_count(), 1);
                drop(locked);
                joined
            };
            drop(callbacks); // Actual same witness callback reenters original authority.
            if let Some(joined) = joined {
                break joined;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(waiting.await.id(), id);
    assert!(Arc::ptr_eq(
        &local.result().unwrap_err(),
        &receipt.result().unwrap_err()
    ));
    let next = weak.spawn(TaskClass::Auxiliary, async { Ok(()) }).unwrap();
    assert!(next.0 > id.0);
    let next = owner.next_receipt().await.unwrap();
    owner.acknowledge(&next).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
