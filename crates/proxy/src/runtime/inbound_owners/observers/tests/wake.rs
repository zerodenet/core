//! Tokio's stored JoinSet waker must not own the real orchestration borrower.
use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Weak,
    },
    task::{Context, Poll, Wake, Waker},
};

fn bounded(work: impl FnOnce() + Send + 'static) {
    let (done, result) = mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let _ = done.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)));
    });
    let result = result
        .recv_timeout(Duration::from_secs(5))
        .expect("original JoinSet caller reentry must remain bounded");
    thread.join().unwrap();
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
#[derive(Default)]
struct Calls {
    wakes: AtomicUsize,
    drops: AtomicUsize,
}
struct Caller {
    registry: Weak<InboundOwnerRegistry>,
    calls: Arc<Calls>,
}
impl Caller {
    fn enter(&self) {
        let registry = self.registry.upgrade().unwrap();
        assert!(
            registry.observers.parked.try_lock().is_ok(),
            "external callback under parking lock"
        );
        assert!(
            registry.take_listener_observers().is_err(),
            "original exclusive lease remains held"
        );
    }
}
impl Wake for Caller {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.enter();
        self.calls.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for Caller {
    fn drop(&mut self) {
        self.enter();
        self.calls.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn caller(registry: &Arc<InboundOwnerRegistry>) -> (Waker, Arc<Calls>) {
    let calls = Arc::new(Calls::default());
    (
        Waker::from(Arc::new(Caller {
            registry: Arc::downgrade(registry),
            calls: calls.clone(),
        })),
        calls,
    )
}

#[test]
fn zero_budget_join_self_wake_reenters_the_original_parking_slot_without_lost_task_id() {
    bounded(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        rt.handle().block_on(async {
            let registry = Arc::new(InboundOwnerRegistry::default());
            let mut original = registry.take_listener_observers().unwrap();
            let handle = original.spawn(async { Err(EngineError::NoInbounds) });
            timeout(Duration::from_secs(2), async {
                while !handle.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let (waker, calls) = caller(&registry);
            let mut joined = Box::pin(original.join_next_with_id());
            while tokio::task::coop::has_budget_remaining() {
                tokio::task::coop::consume_budget().await;
            }
            assert!(joined
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            assert!(
                calls.wakes.load(Ordering::SeqCst) > 0,
                "real zero-budget JoinSet self-wake"
            );
            let actual = tokio::task::unconstrained(joined).await.unwrap().unwrap();
            assert_eq!(actual.0, handle.id());
            assert!(matches!(actual.1, Err(EngineError::NoInbounds)));
            drop(waker); // Reenter while the original lease remains held.
            assert_eq!(calls.drops.load(Ordering::SeqCst), 1);
        });
    });
}

#[test]
fn last_caller_waker_drop_reenters_on_retarget_and_cancel_without_a_parked_strong_cycle() {
    bounded(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        rt.handle().block_on(async {
            let registry = Arc::new(InboundOwnerRegistry::default());
            let mut original = registry.take_listener_observers().unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut io, _) = listener.accept().await.unwrap();
            let (entered, pending) = tokio::sync::oneshot::channel();
            let handle = original.spawn(async move {
                let mut entered = Some(entered);
                let read = io.read_u8();
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(entered) = entered.take() {
                            entered.send(()).unwrap();
                        }
                    }
                    result
                })
                .await?;
                Ok(())
            });
            pending.await.unwrap();
            let (first, calls) = caller(&registry);
            let mut joined = Box::pin(original.join_next());
            assert!(joined
                .as_mut()
                .poll(&mut Context::from_waker(&first))
                .is_pending());
            // The first poll may consume a spawn notification and self-wake.
            // Poll the same real pending task again to store its idle waiter.
            assert!(joined
                .as_mut()
                .poll(&mut Context::from_waker(&first))
                .is_pending());
            drop(first);
            assert_eq!(calls.drops.load(Ordering::SeqCst), 0);
            let (second, replaced) = caller(&registry);
            assert!(joined
                .as_mut()
                .poll(&mut Context::from_waker(&second))
                .is_pending());
            assert_eq!(calls.drops.load(Ordering::SeqCst), 1);
            drop(second);
            drop(joined); // Last actual caller is destroyed before parking.
            assert_eq!(replaced.drops.load(Ordering::SeqCst), 1);
            assert_eq!(original.len(), 1);
            drop(original);
            remote.write_u8(1).await.unwrap();
            let mut resumed = registry.take_listener_observers().unwrap();
            let actual = timeout(Duration::from_secs(2), resumed.join_next_with_id())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(actual.0, handle.id());
            actual.1.unwrap();
        });
    });
}
