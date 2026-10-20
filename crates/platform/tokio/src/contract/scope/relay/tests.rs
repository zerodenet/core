use super::*;
use std::{future::Future, sync::mpsc, task::Context, time::Duration};
use tokio::sync::oneshot;
mod native;

fn bounded(work: impl FnOnce() + Send + 'static) {
    let (done, received) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        let _ = done.send(result);
    });
    let result = received
        .recv_timeout(Duration::from_secs(5))
        .expect("synchronous real-waker reentry must finish without a scope-state deadlock");
    thread.join().unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
struct Reenter {
    registrar: TaskRegistrar,
    result: Mutex<Option<Result<TaskId, SpawnError>>>,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // This acquires the very same real scope mutex synchronously. A mere
        // atomic wake counter would not detect the original recursive deadlock.
        let result = self.registrar.spawn(TaskClass::Auxiliary, async { Ok(()) });
        *self.result.lock().unwrap() = Some(result);
    }
}
fn watcher(registrar: TaskRegistrar) -> (Arc<Reenter>, Waker) {
    let watcher = Arc::new(Reenter {
        registrar,
        result: Mutex::new(None),
    });
    (watcher.clone(), Waker::from(watcher))
}
struct Dropped(Option<oneshot::Sender<()>>);
impl Drop for Dropped {
    fn drop(&mut self) {
        if let Some(send) = self.0.take() {
            let _ = send.send(());
        }
    }
}
fn real_task(registrar: &TaskRegistrar) -> (TaskId, TaskJoinWitness, oneshot::Receiver<()>) {
    let (done, observed) = oneshot::channel();
    let (id, witness) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let _done = Dropped(Some(done));
            Ok(())
        })
        .unwrap();
    (id, witness, observed)
}

#[test]
fn ordinary_native_join_notifies_a_pending_witness_after_unlocking_the_same_registrar() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (id, witness, observed) = real_task(&owner.registrar());
            let (watch, waker) = watcher(owner.registrar());
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            assert!(!witness.is_joined());
            let event = owner.next_exit().await.unwrap();
            assert_eq!(event.id, id);
            assert!(watch.result.lock().unwrap().as_ref().unwrap().is_ok());
            assert_eq!(waiting.await.id(), id);
            let report = owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await;
            assert!(report.is_complete());
            assert_eq!(
                report.exited_tasks.len(),
                1,
                "the real task accepted by the reentrant callback"
            );
        });
    });
}

#[test]
fn registrar_reap_capacity_return_publishes_witness_without_unlocking_its_budget() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (id, witness, observed) = real_task(&owner.registrar());
            let (watch, waker) = watcher(owner.registrar());
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            assert_eq!(
                owner
                    .registrar()
                    .spawn(TaskClass::Auxiliary, async { Ok(()) }),
                Err(SpawnError::Capacity)
            );
            assert_eq!(
                *watch.result.lock().unwrap(),
                Some(Err(SpawnError::Capacity))
            );
            assert_eq!(waiting.await.id(), id);
            assert_eq!(owner.next_exit().await.unwrap().id, id);
            assert!(owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .is_complete());
        });
    });
}

#[test]
fn hosted_actual_poll_notifies_a_pending_witness_without_refunding_unacknowledged_credit() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
            owner.enable_registration().unwrap();
            let (id, witness, observed) = real_task(&owner.registrar());
            let (watch, waker) = watcher(owner.registrar());
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            let event = owner.next_receipt().await.unwrap();
            assert_eq!(event.id(), id);
            assert_eq!(
                *watch.result.lock().unwrap(),
                Some(Err(SpawnError::Capacity))
            );
            assert_eq!(waiting.await.id(), id);
            owner.acknowledge(&event).unwrap();
            owner.begin_shutdown();
            assert!(owner.is_drained());
        });
    });
}

#[test]
fn expired_shutdown_reaps_ready_native_exits_and_notifies_after_the_scope_mutex_is_free() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (id, witness, observed) = real_task(&owner.registrar());
            let (watch, waker) = watcher(owner.registrar());
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            while tokio::task::coop::has_budget_remaining() {
                tokio::task::coop::consume_budget().await;
            }
            // The awaited join must yield for real cooperative budget before
            // the expired timer sends cleanup through its unconstrained reap.
            let report = owner.shutdown(Instant::now()).await;
            assert!(report.is_complete());
            assert_eq!(report.exited_tasks, [id]);
            assert_eq!(
                *watch.result.lock().unwrap(),
                Some(Err(SpawnError::Closing))
            );
            assert_eq!(waiting.await.id(), id);
        });
    });
}
