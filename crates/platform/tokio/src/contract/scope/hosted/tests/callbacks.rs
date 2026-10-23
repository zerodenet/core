use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Wake,
};

struct ParentReentry {
    parent: Arc<Mutex<()>>,
    registrar: TaskRegistrar,
    calls: AtomicUsize,
}
impl Wake for ParentReentry {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _parent = self
            .parent
            .try_lock()
            .expect("parent still locked on callback");
        assert_eq!(
            self.registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity),
            "original delivery remains charged during real notification"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn parent_callback_deferral_keeps_exact_credit_until_real_witness_delivery() {
    let mut owner = scope(1);
    let registrar = owner.registrar();
    let parent = Arc::new(Mutex::new(()));
    for _ in 0..32 {
        let (done, observed) = oneshot::channel();
        let (id, witness) = registrar
            .spawn_tracked(TaskClass::Auxiliary, async move {
                done.send(()).unwrap();
                Ok(())
            })
            .unwrap();
        let callback = Arc::new(ParentReentry {
            parent: parent.clone(),
            registrar: registrar.clone(),
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(callback.clone());
        let mut waiting = Box::pin(witness.wait());
        assert!(waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        observed.await.unwrap();
        let locked = parent.lock().unwrap();
        let callbacks = owner.defer_callbacks();
        let receipt = match owner.poll_exit(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Some(receipt)) => receipt,
            _ => panic!("actual completed native task was not joined"),
        };
        assert_eq!(receipt.id(), id);
        owner.acknowledge(&receipt).unwrap();
        assert_eq!(counts(&owner), (0, 1, 0));
        assert_eq!(callback.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            owner.acknowledge(&receipt),
            Err(HostedReceiptError::StaleReceipt)
        );
        assert_eq!(
            registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        drop(locked);
        drop(callbacks);
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(waiting.await.id(), id);
        assert_eq!(counts(&owner), (0, 0, 0), "no delivery history retained");
    }
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
