use super::*;
use zero_platform_tokio::contract::scope::TaskJoined;

#[test]
fn actual_pending_witness_callback_reenters_the_same_monitor_after_parent_unlock() {
    bounded(|| {
        runtime().block_on(async {
            let monitor = monitor(NonZeroUsize::new(1));
            let registrar = monitor.registrar_for_test();
            let (done, observed) = oneshot::channel();
            let native = Arc::new(Mutex::new(None));
            let actual = native.clone();
            let (id, witness) = registrar
                .spawn_tracked(TaskClass::Critical, async move {
                    *actual.lock().unwrap() = Some(tokio::task::id());
                    done.send(()).unwrap();
                    panic!("actual critical witness/host outer-lock regression");
                })
                .unwrap();
            let (waker, calls) = caller(&monitor, false);
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            let target = target(Waker::noop());
            monitor.poll(&target, instance(), 1);
            assert!(calls.wakes.load(Ordering::SeqCst) > 0);
            assert_eq!(
                *calls.registration.lock().unwrap(),
                Some(Err(SpawnError::Closing))
            );
            let joined = waiting.await;
            assert_eq!(joined.id(), id);
            let source = joined.result().unwrap_err();
            let fault = monitor.fault().unwrap();
            assert_eq!(fault.id, id);
            assert_eq!(fault.class, TaskClass::Critical);
            assert!(Arc::ptr_eq(&fault.source, &source));
            let native_error =
                crate::component::tests::source::<tokio::task::JoinError>(source.as_ref());
            assert!(native_error.is_panic());
            assert_eq!(native_error.id(), native.lock().unwrap().unwrap());
        })
    });
}

struct PanicOnce {
    monitor: Weak<TaskMonitor>,
    pending: AtomicBool,
}
impl Wake for PanicOnce {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let monitor = self.monitor.upgrade().unwrap();
        let _actual = monitor.registrar_for_test();
        if self.pending.swap(false, Ordering::SeqCst) {
            panic!("actual external witness waker panicked once");
        }
    }
}

#[test]
fn callback_panic_retains_original_completed_credit_and_same_failure_for_retry() {
    bounded(|| {
        runtime().block_on(async {
            let monitor = monitor(NonZeroUsize::new(1));
            let registrar = monitor.registrar_for_test();
            let (done, observed) = oneshot::channel();
            let (id, witness) = registrar
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    done.send(()).unwrap();
                    Err(
                        ChannelError::new(Stage::Read, Reason::Io, Impact::Component).with_source(
                            std::io::Error::other("same actual source after callback panic"),
                        ),
                    )
                })
                .unwrap();
            let waker = Waker::from(Arc::new(PanicOnce {
                monitor: Arc::downgrade(&monitor),
                pending: AtomicBool::new(true),
            }));
            let mut waiting = Box::pin(witness.wait());
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            observed.await.unwrap();
            let target = target(Waker::noop());
            let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                monitor.poll(&target, instance(), 1)
            }));
            assert!(failure.is_err());
            assert!(
                monitor.inner.try_lock().is_ok(),
                "callback unwind cannot poison the host owner lock"
            );
            let joined = witness.try_joined().unwrap();
            assert_eq!(joined.id(), id);
            let original = joined.result().unwrap_err();
            assert_eq!(
                registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
                Err(SpawnError::Capacity)
            );
            let local: (TaskJoined, _) = (joined.clone(), original.clone());
            witness.acknowledge_hosted_failure(&local.0).unwrap();
            // The exact charged completion, not a new task or manufactured exit,
            // survives cancellation/unwind and is delivered by the same consumer.
            monitor.poll(&target, instance(), 1);
            assert!(Arc::ptr_eq(
                &original,
                &witness.wait().await.result().unwrap_err()
            ));
            monitor.begin_shutdown();
            assert!(monitor.lock().scope.is_drained());
            assert!(monitor.fault().is_none());
        })
    });
}

#[test]
fn actual_scope_cancellation_waiter_reenters_monitor_only_after_parent_unlock() {
    bounded(|| {
        runtime().block_on(async {
            let monitor = monitor(NonZeroUsize::new(1));
            let registrar = monitor.registrar_for_test();
            let cancellation = registrar.cancellation();
            let mut waiting = Box::pin(cancellation.cancelled());
            let (waker, calls) = caller(&monitor, false);
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            monitor.begin_shutdown();
            assert!(calls.wakes.load(Ordering::SeqCst) > 0);
            assert_eq!(
                *calls.registration.lock().unwrap(),
                Some(Err(SpawnError::Closing))
            );
            waiting.await;
            assert!(monitor.lock().scope.is_drained());
        })
    });
}
