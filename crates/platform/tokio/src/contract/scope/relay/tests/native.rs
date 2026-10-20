use super::*;

struct ReenterDrop {
    registrar: TaskRegistrar,
    dropped: Arc<Mutex<Option<Result<TaskId, SpawnError>>>>,
}
impl Wake for ReenterDrop {
    fn wake(self: Arc<Self>) {
        let _ = self.registrar.spawn(TaskClass::Auxiliary, async { Ok(()) });
    }
}
impl Drop for ReenterDrop {
    fn drop(&mut self) {
        let result = self.registrar.spawn(TaskClass::Auxiliary, async { Ok(()) });
        *self.dropped.lock().unwrap() = Some(result);
    }
}

#[test]
fn replacing_the_actual_join_poll_waker_drops_the_last_external_owner_outside_state() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (release, blocked) = oneshot::channel();
            let (entered, pending) = oneshot::channel();
            let id = owner
                .registrar()
                .spawn(TaskClass::Critical, async move {
                    entered.send(()).unwrap();
                    blocked.await.unwrap();
                    Ok(())
                })
                .unwrap();
            pending.await.unwrap();
            let dropped = Arc::new(Mutex::new(None));
            let waker = Waker::from(Arc::new(ReenterDrop {
                registrar: owner.registrar(),
                dropped: dropped.clone(),
            }));
            let mut waiting = Box::pin(owner.next_exit());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(waker);
            assert!(dropped.lock().unwrap().is_none());
            // Polling with another context replaces the last real external
            // waker held by scope/Notify. Its Drop enters the actual registrar.
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            assert_eq!(*dropped.lock().unwrap(), Some(Err(SpawnError::Capacity)));
            drop(waiting);
            release.send(()).unwrap();
            let report = owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await;
            assert!(report.is_complete());
            assert_eq!(report.exited_tasks, [id]);
        });
    });
}

#[test]
fn actual_joinset_cooperative_self_wake_reenters_the_same_registrar_only_after_state_unlock() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (id, witness, observed) = real_task(&owner.registrar());
            observed.await.unwrap();
            let (watch, waker) = watcher(owner.registrar());
            let mut waiting = Box::pin(owner.next_exit());
            while tokio::task::coop::has_budget_remaining() {
                tokio::task::coop::consume_budget().await;
            }
            // The real JoinHandle is ready, but the current actual Tokio task
            // has no cooperative budget. JoinSet's own self-wake must go via
            // the metadata-only relay, not call this external waker in State.
            let event = match waiting.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(Some(event)) => event,
                Poll::Pending => waiting.as_mut().await.unwrap(),
                Poll::Ready(None) => panic!("the real task exit was lost"),
            };
            drop(waiting);
            assert_eq!(event.id, id);
            assert_eq!(
                *watch.result.lock().unwrap(),
                Some(Err(SpawnError::Capacity))
            );
            assert_eq!(witness.wait().await.id(), id);
            assert!(owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .is_complete());
        });
    });
}

#[test]
fn joinset_retained_internal_relay_cannot_keep_a_scope_or_its_external_owner_alive() {
    bounded(|| {
        let rt = runtime();
        rt.block_on(async {
            let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
            let (entered, pending) = oneshot::channel();
            let (dropped, observed) = oneshot::channel();
            let (_, witness) = owner
                .registrar()
                .spawn_tracked(TaskClass::Critical, async move {
                    let _body = Dropped(Some(dropped));
                    entered.send(()).unwrap();
                    std::future::pending::<Result<(), ComponentError>>().await
                })
                .unwrap();
            pending.await.unwrap();
            let mut waiting = Box::pin(owner.next_exit());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            drop(waiting);
            let weak = Arc::downgrade(&owner.0);
            let relay = owner.0.relay.clone();
            drop(owner);
            observed.await.unwrap();
            assert!(
                weak.upgrade().is_none(),
                "relay holds neither Scope nor Shared nor future ownership"
            );
            relay.signal(); // Its weak notification targets have gone away.
            assert!(
                !witness.is_joined(),
                "emergency abort never issues a real join receipt"
            );
        });
    });
}
