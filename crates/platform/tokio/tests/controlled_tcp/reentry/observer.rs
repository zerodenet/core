use super::*;
#[derive(Default)]
struct Reenter {
    shared: Mutex<Weak<Shared>>,
    reads: AtomicUsize,
    writes: AtomicUsize,
    errors: AtomicUsize,
    boundaries: AtomicUsize,
    close_on_write: AtomicBool,
    actual_close: AtomicBool,
    panic_read: AtomicBool,
}
impl std::fmt::Debug for Reenter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Reenter")
    }
}
impl zero_traits::IoObserver for Reenter {
    fn stream_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::SeqCst);
    }
    fn received(&self, bytes: usize) {
        self.reads.fetch_add(bytes, Ordering::SeqCst);
        let shared = self.shared.lock().unwrap().upgrade().unwrap();
        assert_unlocked(&shared);
        assert!(shared.local_addr().unwrap().ip().is_loopback());
        assert!(
            !self.panic_read.load(Ordering::SeqCst),
            "original observer read panic"
        );
    }
    fn sent(&self, bytes: usize) {
        self.writes.fetch_add(bytes, Ordering::SeqCst);
        let shared = self.shared.lock().unwrap().upgrade().unwrap();
        assert_unlocked(&shared);
        assert!(shared.peer_addr().unwrap().ip().is_loopback());
        if self.close_on_write.swap(false, Ordering::SeqCst) {
            let report = close_once(&shared);
            assert!(report.is_complete());
            assert_eq!(report.closed_io.len(), 1);
            self.actual_close.store(true, Ordering::SeqCst);
        }
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
        let shared = self.shared.lock().unwrap().upgrade().unwrap();
        assert_unlocked(&shared);
        shared.cancel.cancel();
    }
    fn dropped(&self) {}
}
#[tokio::test]
async fn original_tokio_and_pending_native_poll_counts_survive_synchronous_observer_close() {
    timeout(Duration::from_secs(5), async {
        for native in [false, true] {
            let observer = Arc::new(Reenter::default());
            observer.close_on_write.store(true, Ordering::SeqCst);
            let (mut io, mut control, mut peer) = if native {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let egress = crate::EgressInterfaceControl::default();
                let selection = egress
                    .select_for_peer_with_policy(address, &Default::default())
                    .unwrap();
                let (mut pending, control) = PendingTcpConnect::prepare(
                    address,
                    &Default::default(),
                    &selection,
                    CancellationToken::new(),
                    Some(observer.clone()),
                )
                .unwrap();
                *observer.shared.lock().unwrap() = Arc::downgrade(&control.shared);
                let (io, peer) = tokio::join!(pending.connect(end()), listener.accept());
                (io.unwrap(), control, peer.unwrap().0)
            } else {
                let (socket, peer) = pair().await;
                let (io, control) = socket
                    .with_observer(Some(observer.clone()))
                    .into_controlled(CancellationToken::new())
                    .unwrap();
                *observer.shared.lock().unwrap() = Arc::downgrade(&control.shared);
                (io, control, peer)
            };
            io.write_all(b"original bytes").await.unwrap();
            let mut received = Vec::new();
            peer.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, b"original bytes");
            assert!(observer.actual_close.load(Ordering::SeqCst));
            assert_eq!(observer.writes.load(Ordering::SeqCst), 14);
            assert_eq!(observer.boundaries.load(Ordering::SeqCst), 1);
            assert_eq!(observer.errors.load(Ordering::SeqCst), 0);
            assert!(control.wait_closed(end()).await.unwrap().is_complete());
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn real_read_observer_panic_restores_original_socket_without_poison_or_close_claim() {
    timeout(Duration::from_secs(5), async {
        let (socket, mut peer) = pair().await;
        let observer = Arc::new(Reenter::default());
        observer.panic_read.store(true, Ordering::SeqCst);
        let (mut io, mut control) = socket
            .with_observer(Some(observer.clone()))
            .into_controlled(CancellationToken::new())
            .unwrap();
        *observer.shared.lock().unwrap() = Arc::downgrade(&control.shared);
        peer.write_all(b"x").await.unwrap();
        let shared = control.shared.clone();
        let worker = tokio::spawn(async move {
            let mut byte = [0];
            io.read_exact(&mut byte).await.unwrap();
        });
        assert!(worker.await.unwrap_err().is_panic());
        assert_unlocked(&shared);
        assert!(!shared.state().closed);
        assert!(shared.state().socket.is_some());
        assert_eq!(observer.reads.load(Ordering::SeqCst), 1);
        assert_eq!(observer.errors.load(Ordering::SeqCst), 0);
        assert_eq!(
            peer.try_read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let report = control.wait_closed(end()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_reset_preserves_one_original_error_callback_outside_owner_lock() {
    timeout(Duration::from_secs(5), async {
        let (socket, peer) = pair().await;
        let observer = Arc::new(Reenter::default());
        let (mut io, mut control) = socket
            .with_observer(Some(observer.clone()))
            .into_controlled(CancellationToken::new())
            .unwrap();
        *observer.shared.lock().unwrap() = Arc::downgrade(&control.shared);
        socket2::SockRef::from(&peer)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(peer);
        let failure = io.read(&mut [0; 1]).await.unwrap_err();
        assert!(matches!(
            failure.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
        ));
        assert_eq!(observer.errors.load(Ordering::SeqCst), 1);
        assert_eq!(observer.reads.load(Ordering::SeqCst), 0);
        assert!(!control.shared.state().closed);
        assert!(control.wait_closed(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}
