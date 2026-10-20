use super::*;
use std::task::{RawWaker, RawWakerVTable};
#[derive(Clone, Copy)]
enum Event {
    Clone,
    Drop,
    Wake,
}
struct Reenter {
    shared: Mutex<Weak<Shared>>,
    close_drop: AtomicBool,
    close_wake: AtomicBool,
    callbacks: AtomicUsize,
    unfinished: AtomicUsize,
    actual_closed: AtomicUsize,
    local: Mutex<Option<SocketAddr>>,
}
impl Reenter {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shared: Mutex::new(Weak::new()),
            close_drop: AtomicBool::new(false),
            close_wake: AtomicBool::new(false),
            callbacks: AtomicUsize::new(0),
            unfinished: AtomicUsize::new(0),
            actual_closed: AtomicUsize::new(0),
            local: Mutex::new(None),
        })
    }
    fn action(&self, event: Event) {
        let shared = self.shared.lock().unwrap().upgrade();
        let Some(shared) = shared else {
            return;
        };
        assert_unlocked(&shared);
        self.callbacks.fetch_add(1, Ordering::SeqCst);
        *self.local.lock().unwrap() = shared.local_addr().ok();
        let close = match event {
            Event::Drop => self.close_drop.swap(false, Ordering::SeqCst),
            Event::Wake => self.close_wake.swap(false, Ordering::SeqCst),
            Event::Clone => false,
        };
        if close {
            let report = close_once(&shared);
            if report.is_complete() {
                assert_eq!(report.closed_io.len(), 1);
                self.actual_closed.fetch_add(1, Ordering::SeqCst);
            } else {
                assert!(report.closed_io.is_empty());
                assert!(!shared.state().closed);
                assert!(shared.state().polling);
                self.unfinished.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}
unsafe fn clone_raw(data: *const ()) -> RawWaker {
    // SAFETY: every raw value owns one Arc; cloning keeps this value's ownership.
    let original = std::mem::ManuallyDrop::new(unsafe { Arc::<Reenter>::from_raw(data.cast()) });
    original.action(Event::Clone);
    RawWaker::new(Arc::into_raw(Arc::clone(&original)).cast(), &VTABLE)
}
unsafe fn wake_raw(data: *const ()) {
    // SAFETY: consuming wake takes this raw value's unique Arc reference.
    let original = unsafe { Arc::<Reenter>::from_raw(data.cast()) };
    original.action(Event::Wake);
}
unsafe fn wake_ref(data: *const ()) {
    // SAFETY: borrowed wake leaves this raw value's Arc reference intact.
    let original = std::mem::ManuallyDrop::new(unsafe { Arc::<Reenter>::from_raw(data.cast()) });
    original.action(Event::Wake);
}
unsafe fn drop_raw(data: *const ()) {
    // SAFETY: drop consumes exactly this raw value's Arc reference.
    let original = unsafe { Arc::<Reenter>::from_raw(data.cast()) };
    original.action(Event::Drop);
}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_ref, drop_raw);
fn waker(probe: Arc<Reenter>) -> Waker {
    // SAFETY: the table applies the matching Arc ownership operations above.
    unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE)) }
}
#[tokio::test]
async fn pre_transfer_original_tokio_waker_drop_reenters_inflight_metadata_without_false_close() {
    timeout(Duration::from_secs(5), async {
        let (mut socket, mut peer) = pair().await;
        let local: SocketAddr = "192.0.2.8:8080".parse().unwrap();
        socket.set_effective_addresses("198.51.100.9:9".parse().unwrap(), local);
        let probe = Reenter::new();
        let old = waker(probe.clone());
        let mut bytes = [0];
        let mut buffer = ReadBuf::new(&mut bytes);
        assert!(Pin::new(&mut socket)
            .poll_read(&mut Context::from_waker(&old), &mut buffer)
            .is_pending());
        drop(old); // the real original Tokio registration retains its clone
        let (mut io, mut control) = socket.into_controlled(CancellationToken::new()).unwrap();
        *probe.shared.lock().unwrap() = Arc::downgrade(&control.shared);
        probe.close_drop.store(true, Ordering::SeqCst);
        let mut buffer = ReadBuf::new(&mut bytes);
        let result =
            Pin::new(&mut io).poll_read(&mut Context::from_waker(Waker::noop()), &mut buffer);
        assert!(result.is_pending() || matches!(result, Poll::Ready(Err(_))));
        assert!(
            probe.callbacks.load(Ordering::SeqCst) > 0,
            "the old original registration really dropped an external waker"
        );
        assert_eq!(probe.unfinished.load(Ordering::SeqCst), 1);
        assert_eq!(probe.actual_closed.load(Ordering::SeqCst), 0);
        assert_eq!(*probe.local.lock().unwrap(), Some(local));
        assert!(control.shared.state().socket.is_some());
        assert!(!control.shared.state().polling);
        assert!(!control.shared.state().closed);
        assert!(control.shared.state().unknown.is_none());
        assert_eq!(io.local_addr().unwrap(), local);
        assert!(control.wait_closed(end()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[test]
fn real_pending_read_cooperative_self_wake_restores_socket_before_external_close() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    // Drive this real budgeted caller without a scheduler Context, so pinned
    // Tokio's context::defer uses its synchronous wake path. The single runtime
    // worker continues to drive actual loopback registration and readiness.
    runtime.handle().block_on(async {
        timeout(Duration::from_secs(5), async {
            let (socket, mut peer) = pair().await;
            let (mut io, mut control) = socket.into_controlled(CancellationToken::new()).unwrap();
            let probe = Reenter::new();
            *probe.shared.lock().unwrap() = Arc::downgrade(&control.shared);
            let external = waker(probe.clone());
            let mut bytes = [0];
            {
                let mut buffer = ReadBuf::new(&mut bytes);
                assert!(Pin::new(&mut io)
                    .poll_read(&mut Context::from_waker(&external), &mut buffer)
                    .is_pending());
            }
            let mut exhausted = false;
            for _ in 0..1024 {
                let mut budget = Box::pin(tokio::task::coop::consume_budget());
                if budget
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
                {
                    exhausted = true;
                    break;
                }
            }
            assert!(exhausted);
            assert!(!tokio::task::coop::has_budget_remaining());
            probe.close_wake.store(true, Ordering::SeqCst);
            let mut buffer = ReadBuf::new(&mut bytes);
            assert!(Pin::new(&mut io)
                .poll_read(&mut Context::from_waker(&external), &mut buffer)
                .is_pending());
            assert_eq!(probe.actual_closed.load(Ordering::SeqCst), 1);
            assert_eq!(probe.unfinished.load(Ordering::SeqCst), 0);
            assert!(control.shared.state().closed);
            assert!(control.shared.state().socket.is_none());
            assert!(probe.local.lock().unwrap().is_some());
            assert!(control.wait_closed(end()).await.unwrap().is_complete());
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        })
        .await
        .unwrap();
    });
}
#[tokio::test]
async fn replacing_and_dropping_caller_waker_never_holds_state_or_retains_the_owner() {
    let (socket, mut peer) = pair().await;
    let (mut io, mut control) = socket.into_controlled(CancellationToken::new()).unwrap();
    let shared = Arc::downgrade(&control.shared);
    let probe = Reenter::new();
    *probe.shared.lock().unwrap() = shared.clone();
    let external = waker(probe.clone());
    let mut bytes = [0];
    assert!(Pin::new(&mut io)
        .poll_read(
            &mut Context::from_waker(&external),
            &mut ReadBuf::new(&mut bytes)
        )
        .is_pending());
    drop(external);
    probe.close_drop.store(true, Ordering::SeqCst);
    assert!(matches!(
        Pin::new(&mut io).poll_read(
            &mut Context::from_waker(Waker::noop()),
            &mut ReadBuf::new(&mut bytes)
        ),
        Poll::Ready(Err(_))
    ));
    assert_eq!(probe.actual_closed.load(Ordering::SeqCst), 1);
    assert!(control.wait_closed(end()).await.unwrap().is_complete());
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    drop(io);
    drop(control);
    assert!(
        shared.upgrade().is_none(),
        "native relays retain no owner/future"
    );
}
