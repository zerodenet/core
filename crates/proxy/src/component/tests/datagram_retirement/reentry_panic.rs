use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_pending_close_poll_panic_preserves_the_same_controller_and_never_acknowledges_closed(
) {
    let owner = owner(1);
    let (actual, eof, release, server) = tcp_control(false).await;
    let pending = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = owner.reserve().unwrap().adopt(Box::new(Control {
        actual,
        owner: Arc::downgrade(&owner),
        cancel_calls: Arc::new(AtomicUsize::new(0)),
        pending: pending.clone(),
        sync_wake: false,
        cancel_panic: Arc::new(AtomicBool::new(false)),
        poll_panic: Arc::new(AtomicBool::new(true)),
        drops: drops.clone(),
    }));
    drop(lease);
    let actual_owner = owner.clone();
    let runtime = tokio::runtime::Handle::current();
    bounded(move || {
        let _runtime = runtime.enter();
        let mut closing = Box::pin(actual_owner.progress_retired(deadline()));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            closing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        }))
        .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"original pending-controller poll panic")
        );
    });
    tokio::time::timeout_at(deadline(), eof.notified())
        .await
        .unwrap();
    assert!(pending.load(Ordering::SeqCst));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
    let report = owner
        .progress_retired(tokio::time::Instant::now() + std::time::Duration::from_millis(5))
        .await
        .unwrap();
    assert!(!report.is_complete());
    assert!(report.closed_io.is_empty());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the protocol future cannot drop its sole original controller on panic"
    );
    release.notify_one();
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    // The peer's EOF does not repair a panicked protocol cleanup future. Its
    // original controller stays charged; no generic retry invents a close ACK.
    assert_eq!(owner.reserve().err().unwrap().reason, Reason::Capacity);
}
