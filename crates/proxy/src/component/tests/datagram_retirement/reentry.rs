//! Real controllers retain their original TCP I/O across callback boundaries.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    BoxFuture, ChannelError, CloseReason,
};
fn bounded<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let (done, read) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        let _ = done.send(result);
    });
    let result = read
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("synchronous callback reentry must not deadlock");
    thread.join().unwrap();
    match result {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
fn probe(owner: &Weak<RetiredDatagrams>) {
    let owner = owner.upgrade().expect("actual lifetime owner");
    let _ = format!("{owner:?}");
    assert_eq!(
        owner.entries().len(),
        1,
        "the same controller still occupies its entry"
    );
    assert!(matches!(
        owner.reserve().err().unwrap().reason,
        Reason::Capacity | Reason::Revoked
    ));
}
struct Control {
    actual: Box<dyn DynChannelControl>,
    owner: Weak<RetiredDatagrams>,
    cancel_calls: Arc<AtomicUsize>,
    pending: Arc<AtomicBool>,
    sync_wake: bool,
    cancel_panic: Arc<AtomicBool>,
    poll_panic: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}
impl Drop for Control {
    fn drop(&mut self) {
        if self.owner.upgrade().is_some() {
            probe(&self.owner);
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        probe(&self.owner);
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        if self.cancel_panic.swap(false, Ordering::SeqCst) {
            panic!("original actual-controller cancel callback panic")
        }
        self.actual.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        let owner = self.owner.clone();
        let pending = self.pending.clone();
        let mut wake = self.sync_wake;
        let poll_panic = self.poll_panic.clone();
        let mut actual = self.actual.wait_closed(deadline);
        Box::pin(std::future::poll_fn(move |cx| {
            probe(&owner);
            let result = actual.as_mut().poll(cx);
            if result.is_pending() {
                pending.store(true, Ordering::SeqCst);
                if poll_panic.swap(false, Ordering::SeqCst) {
                    panic!("original pending-controller poll panic")
                }
                if wake {
                    wake = false;
                    cx.waker().wake_by_ref();
                }
            }
            result
        }))
    }
}
struct Callback {
    owner: Weak<RetiredDatagrams>,
    pending: Arc<AtomicBool>,
    wakes: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Wake for Callback {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.pending.load(Ordering::SeqCst),
            "the actual TCP read is Pending"
        );
        probe(&self.owner);
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for Callback {
    fn drop(&mut self) {
        probe(&self.owner);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
type Closing = Pin<Box<dyn Future<Output = Result<CloseReport, ChannelError>> + Send>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_pending_tcp_cancel_poll_wake_and_last_caller_waker_drop_reenter_without_parent_locks(
) {
    let owner = owner(1);
    let (actual, eof, release, server) = tcp_control(false).await;
    let pending = Arc::new(AtomicBool::new(false));
    let cancels = Arc::new(AtomicUsize::new(0));
    let lease = owner.reserve().unwrap().adopt(Box::new(Control {
        actual,
        owner: Arc::downgrade(&owner),
        cancel_calls: cancels.clone(),
        pending: pending.clone(),
        sync_wake: true,
        cancel_panic: Arc::new(AtomicBool::new(false)),
        poll_panic: Arc::new(AtomicBool::new(false)),
        drops: Arc::new(AtomicUsize::new(0)),
    }));
    bounded(move || drop(lease));
    assert_eq!(cancels.load(Ordering::SeqCst), 1);
    let wakes = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let (host, waiting, actual_pending, actual_wakes, actual_drops) = (
        owner.clone(),
        owner.clone(),
        pending.clone(),
        wakes.clone(),
        drops.clone(),
    );
    let replacement_drops = Arc::new(AtomicUsize::new(0));
    let actual_replacement_drops = replacement_drops.clone();
    let runtime = tokio::runtime::Handle::current();
    let closing = bounded(move || {
        let _runtime = runtime.enter();
        let waker = Waker::from(Arc::new(Callback {
            owner: Arc::downgrade(&host),
            pending: actual_pending.clone(),
            wakes: actual_wakes,
            drops: actual_drops.clone(),
        }));
        let mut closing: Closing = Box::pin(async move { waiting.wait_closed(deadline()).await });
        assert!(closing
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        // Re-register after the synchronous wake consumed the first target.
        assert!(closing
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        drop(waker);
        let replacement = Waker::from(Arc::new(Callback {
            owner: Arc::downgrade(&host),
            pending: actual_pending,
            wakes: Arc::new(AtomicUsize::new(0)),
            drops: actual_replacement_drops,
        }));
        assert!(closing
            .as_mut()
            .poll(&mut Context::from_waker(&replacement))
            .is_pending());
        assert_eq!(
            actual_drops.load(Ordering::SeqCst),
            1,
            "replacement dropped the last original caller waker outside parent locks"
        );
        closing
    });
    tokio::time::timeout_at(deadline(), eof.notified())
        .await
        .unwrap();
    assert!(pending.load(Ordering::SeqCst));
    assert!(wakes.load(Ordering::SeqCst) > 0);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the first caller target was replaced"
    );
    bounded(move || drop(closing));
    assert_eq!(replacement_drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Revoked);
    release.notify_one();
    let report = owner.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_cancel_callback_panic_propagates_same_payload_and_retains_original_controller_for_retry(
) {
    let owner = owner(1);
    let (actual, eof, release, server) = tcp_control(false).await;
    let cancels = Arc::new(AtomicUsize::new(0));
    let lease = owner.reserve().unwrap().adopt(Box::new(Control {
        actual,
        owner: Arc::downgrade(&owner),
        cancel_calls: cancels.clone(),
        pending: Arc::new(AtomicBool::new(false)),
        sync_wake: false,
        cancel_panic: Arc::new(AtomicBool::new(true)),
        poll_panic: Arc::new(AtomicBool::new(false)),
        drops: Arc::new(AtomicUsize::new(0)),
    }));
    let lease = bounded(move || {
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            lease.cancel(CloseReason::Shutdown)
        }))
        .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"original actual-controller cancel callback panic")
        );
        lease
    });
    assert_eq!(cancels.load(Ordering::SeqCst), 1);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    let progressing = owner.progress_retired(deadline());
    tokio::pin!(progressing);
    tokio::select! {_=eof.notified()=>{},result=&mut progressing=>panic!("real peer EOF is still gated: {result:?}")}
    release.notify_one();
    let report = progressing.await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    drop(lease);
    drop(owner.reserve().unwrap());
}

#[path = "reentry_panic.rs"]
mod panic;
