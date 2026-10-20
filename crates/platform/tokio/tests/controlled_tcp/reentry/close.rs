use super::*;
struct ReenterDrop {
    shared: Mutex<Weak<Shared>>,
    observed: Arc<AtomicUsize>,
    panic: bool,
}
impl std::fmt::Debug for ReenterDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReenterDrop")
    }
}
impl zero_traits::IoObserver for ReenterDrop {
    fn stream_boundary(&self) {}
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
impl Drop for ReenterDrop {
    fn drop(&mut self) {
        let shared = self.shared.lock().unwrap().upgrade().unwrap();
        assert_unlocked(&shared);
        assert!(
            shared.state().closed,
            "the actual disposition is durable before observer Drop"
        );
        assert!(shared.state().socket.is_none());
        assert!(shared.state().unknown.is_none());
        shared.cancel.cancel();
        self.observed.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic,
            "original observer destructor panic after native close"
        );
    }
}
#[tokio::test]
async fn original_observer_destructor_reenters_only_after_durable_native_close() {
    timeout(Duration::from_secs(5), async {
        for panic in [false, true] {
            let (socket, mut peer) = pair().await;
            let observed = Arc::new(AtomicUsize::new(0));
            let observer = Arc::new(ReenterDrop {
                shared: Mutex::new(Weak::new()),
                observed: observed.clone(),
                panic,
            });
            let (io, mut control) = socket
                .with_observer(Some(observer.clone()))
                .into_controlled(CancellationToken::new())
                .unwrap();
            *observer.shared.lock().unwrap() = Arc::downgrade(&control.shared);
            drop(observer);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut close = Box::pin(control.wait_closed(end()));
                close.as_mut().poll(&mut Context::from_waker(Waker::noop()))
            }));
            assert_eq!(result.is_err(), panic);
            assert_eq!(observed.load(Ordering::SeqCst), 1);
            assert!(control.shared.state().closed);
            assert!(control.shared.state().unknown.is_none());
            let report = control.wait_closed(end()).await.unwrap();
            assert!(report.is_complete());
            assert_eq!(report.closed_io.len(), usize::from(panic));
            assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
            drop(io);
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn synchronous_socket_borrow_restores_same_original_after_native_operation_panic() {
    timeout(Duration::from_secs(5), async {
        let (socket, mut peer) = pair().await;
        let local = socket.local_addr().unwrap();
        let (mut io, mut control) = socket.into_controlled(CancellationToken::new()).unwrap();
        let shared = control.shared.clone();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _borrow = shared.lease().unwrap();
            assert_eq!(shared.local_addr().unwrap(), local);
            let report = close_once(&shared);
            assert!(!report.is_complete());
            assert!(report.closed_io.is_empty());
            panic!("unobserved synchronous native poll panic");
        }));
        assert!(panic.is_err());
        assert!(!shared.state().closed);
        assert!(!shared.state().polling);
        assert!(shared.state().socket.is_some());
        assert!(shared.state().unknown.is_none());
        assert_eq!(io.local_addr().unwrap(), local);
        // Retirement cancellation above stays terminal; no byte retry is added.
        assert_eq!(
            io.write_all(b"late").await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(control.wait_closed(end()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
