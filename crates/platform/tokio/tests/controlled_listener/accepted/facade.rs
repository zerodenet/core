use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[derive(Debug, Default)]
struct Bytes {
    read: AtomicUsize,
    write: AtomicUsize,
    boundaries: AtomicUsize,
}
impl zero_traits::IoObserver for Bytes {
    fn stream_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::Relaxed);
    }
    fn received(&self, bytes: usize) {
        self.read.fetch_add(bytes, Ordering::Relaxed);
    }
    fn sent(&self, bytes: usize) {
        self.write.fetch_add(bytes, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn accepted_facade_keeps_original_io_metadata_peek_and_observation_without_raw_escape() {
    timeout(Duration::from_secs(3), async {
        let (mut owner, mut listener, address) = bound();
        let (mut peer, io, claim) = pair(&mut listener, address).await;
        let observer = Arc::new(Bytes::default());
        let socket = TokioSocket::from_accepted(io).with_observer(Some(observer.clone()));
        assert_eq!(socket.local_addr().unwrap(), address);
        assert_eq!(socket.peer_addr().unwrap(), peer.local_addr().unwrap());
        let socket = socket
            .into_inner()
            .expect_err("accepted native original must stay in its entry");
        let mut socket = (*socket)
            .into_controlled(CancellationToken::new())
            .err()
            .expect("an accepted view cannot create a second control");
        let source = "192.0.2.4:4000".parse().unwrap();
        let destination = "192.0.2.5:5000".parse().unwrap();
        socket.set_effective_addresses(source, destination);
        assert_eq!(socket.peer_addr().unwrap(), source);
        assert_eq!(socket.local_addr().unwrap(), destination);
        peer.write_all(b"abc").await.unwrap();
        let mut bytes = [0; 3];
        assert_eq!(socket.peek(&mut bytes).await.unwrap(), 3);
        assert_eq!(&bytes, b"abc");
        assert_eq!(
            observer.read.load(Ordering::Relaxed),
            0,
            "peek is not received payload"
        );
        socket.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"abc");
        socket.write_all(b"def").await.unwrap();
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"def");
        assert_eq!(observer.read.load(Ordering::Relaxed), 3);
        assert_eq!(observer.write.load(Ordering::Relaxed), 3);
        assert_eq!(observer.boundaries.load(Ordering::Relaxed), 1);
        let receipt = owner
            .close_accepted(&claim, deadline())
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.matches(&claim));
        assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
        assert_eq!(
            socket.peek(&mut bytes).await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            socket.read(&mut bytes).await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert!(!owner.wait_closed(deadline()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pending_accepted_peek_cancels_without_consuming_or_closing_the_original() {
    let (mut owner, mut listener, address) = bound();
    let (mut peer, io, claim) = pair(&mut listener, address).await;
    let socket = TokioSocket::from_accepted(io);
    let mut byte = [0];
    let mut pending = Box::pin(socket.peek(&mut byte));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    drop(pending);
    assert_eq!(owner.accepted_evidence().unwrap().retained, 1);
    assert!(owner.take_accepted_report().closed_io.is_empty());
    peer.write_all(b"z").await.unwrap();
    assert_eq!(socket.peek(&mut byte).await.unwrap(), 1);
    assert_eq!(byte, [b'z']);
    let mut socket = socket;
    socket.read_exact(&mut byte).await.unwrap();
    drop(socket);
    assert_eq!(
        peer.try_read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    owner.close_accepted(&claim, deadline()).await.unwrap();
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    owner.wait_closed(deadline()).await.unwrap();
}
