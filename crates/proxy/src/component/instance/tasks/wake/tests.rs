use super::*;
use crate::component::instance::TaskMonitor;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::{runtime::Handle, sync::oneshot};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration, TaskId};
use zero_platform_tokio::contract::{
    scope::{SpawnError, TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
mod witness;

fn bounded(work: impl FnOnce() + Send + 'static) {
    let (done, result) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)));
    });
    let result = result
        .recv_timeout(Duration::from_secs(5))
        .expect("actual caller reentry must not deadlock the host monitor");
    thread.join().unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(201),
        generation: ResourceGeneration(1),
    }
}
fn monitor(limit: Option<NonZeroUsize>) -> Arc<TaskMonitor> {
    let monitor = Arc::new(TaskMonitor::new(TaskScope::suspended_hosted(
        Handle::current(),
        limit,
    )));
    monitor.enable_registration().unwrap();
    monitor
}
#[derive(Default)]
struct Calls {
    wakes: AtomicUsize,
    drops: AtomicUsize,
    attempted: AtomicBool,
    registration: Mutex<Option<Result<TaskId, SpawnError>>>,
}
struct Reenter {
    monitor: Weak<TaskMonitor>,
    calls: Arc<Calls>,
    on_drop: bool,
}
impl Reenter {
    fn touch(&self) {
        if let Some(monitor) = self.monitor.upgrade() {
            assert!(
                monitor.inner.try_lock().is_ok(),
                "caller callback retained the enclosing monitor lock"
            );
            // Actually enter the same monitor, not only an atomic wake count.
            let registrar = monitor.registrar_for_test();
            if !self.calls.attempted.swap(true, Ordering::SeqCst) {
                let result = registrar.spawn(TaskClass::Auxiliary, async { Ok(()) });
                *self.calls.registration.lock().unwrap() = Some(result);
            }
        }
    }
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.wakes.fetch_add(1, Ordering::SeqCst);
        self.touch();
    }
}
impl Drop for Reenter {
    fn drop(&mut self) {
        if self.on_drop {
            self.touch();
            self.calls.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}
fn caller(monitor: &Arc<TaskMonitor>, on_drop: bool) -> (Waker, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    (
        Waker::from(Arc::new(Reenter {
            monitor: Arc::downgrade(monitor),
            calls: calls.clone(),
            on_drop,
        })),
        calls,
    )
}
fn target(waker: &Waker) -> Arc<WaitTarget> {
    let target = Arc::new(WaitTarget::default());
    target.register(&Context::from_waker(waker));
    target
}

#[test]
fn actual_zero_budget_join_self_wake_reenters_monitor_only_after_both_locks_release() {
    bounded(|| {
        // Handle::block_on is outside the worker scheduler's deferred-wake
        // context. Keep its one real worker alive to drive the original task.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        rt.handle().block_on(async {
            let monitor = monitor(NonZeroUsize::new(1));
            let registrar = monitor.registrar_for_test();
            let (done, observed) = oneshot::channel();
            let (id, witness) = registrar
                .spawn_tracked(TaskClass::Auxiliary, async move {
                    done.send(()).unwrap();
                    Err(
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(std::io::Error::other("actual completed native task")),
                    )
                })
                .unwrap();
            observed.await.unwrap();
            let (waker, calls) = caller(&monitor, false);
            let target = target(&waker);
            while tokio::task::coop::has_budget_remaining() {
                tokio::task::coop::consume_budget().await;
            }
            monitor.poll(&target, instance(), 1);
            assert!(calls.wakes.load(Ordering::SeqCst) > 0);
            assert_eq!(
                *calls.registration.lock().unwrap(),
                Some(Err(SpawnError::Capacity))
            );
            let joined = tokio::task::unconstrained(std::future::poll_fn(|cx| {
                // The worker may still be finishing after its oneshot send.
                // Await the same actual native exit rather than assuming Ready.
                target.register(cx);
                monitor.poll(&target, instance(), 1);
                match witness.try_joined() {
                    Some(joined) => Poll::Ready(joined),
                    None => Poll::Pending,
                }
            }))
            .await;
            assert_eq!(joined.id(), id);
            assert_eq!(joined.class(), TaskClass::Auxiliary);
            let original = joined.result().unwrap_err();
            assert!(monitor.fault().is_none());
            let local = (joined.clone(), original.clone());
            witness.acknowledge_hosted_failure(&local.0).unwrap();
            tokio::task::unconstrained(async {
                monitor.poll(&target, instance(), 1);
            })
            .await;
            assert!(Arc::ptr_eq(&original, &local.0.result().unwrap_err()));
            monitor.begin_shutdown();
            assert!(monitor.lock().scope.is_drained());
        })
    });
}

#[test]
fn last_external_waker_drop_reenters_monitor_during_caller_replacement_and_cancellation() {
    bounded(|| {
        runtime().block_on(async {
            let monitor = monitor(None);
            let (waker, calls) = caller(&monitor, true);
            let target = target(&waker);
            monitor.poll(&target, instance(), 1);
            drop(waker);
            assert_eq!(calls.drops.load(Ordering::SeqCst), 0);
            let (replacement, second) = caller(&monitor, true);
            target.register(&Context::from_waker(&replacement));
            assert_eq!(calls.drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                *calls.registration.lock().unwrap(),
                Some(Err(SpawnError::Capacity))
            );
            drop(replacement);
            let weak = Arc::downgrade(&target);
            drop(target);
            assert!(
                weak.upgrade().is_none(),
                "neither monitor nor native Scope retains caller ownership"
            );
            assert_eq!(second.drops.load(Ordering::SeqCst), 1);
            monitor.begin_shutdown();
            assert!(monitor.lock().scope.is_drained());
        })
    });
}
