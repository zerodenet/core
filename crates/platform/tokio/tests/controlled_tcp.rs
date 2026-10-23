use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};
use zero_platform_tokio::{
    contract::{lifecycle::ChannelControl, CancellationToken, CloseReason},
    ControlledTcpStream, TokioSocket,
};
#[path = "controlled_tcp/descriptor.rs"]
mod descriptor;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
async fn pair() -> (TokioSocket, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (socket, peer) = tokio::join!(
        TokioSocket::connect_addr(listener.local_addr().unwrap()),
        listener.accept()
    );
    (socket.unwrap(), peer.unwrap().0)
}
#[derive(Debug, Default)]
struct Observer {
    read: AtomicUsize,
    written: AtomicUsize,
    errors: AtomicUsize,
    boundaries: AtomicUsize,
}
impl zero_traits::IoObserver for Observer {
    fn stream_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::Relaxed);
    }
    fn received(&self, n: usize) {
        self.read.fetch_add(n, Ordering::Relaxed);
    }
    fn sent(&self, n: usize) {
        self.written.fetch_add(n, Ordering::Relaxed);
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn dropped(&self) {}
}

#[tokio::test]
async fn controlled_tcp_half_close_preserves_reverse_bytes_observer_and_closes_original_descriptor()
{
    timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = pair().await;
        let observer = Arc::new(Observer::default());
        let local = socket.local_addr().unwrap();
        let remote = socket.peer_addr().unwrap();
        let (mut io, mut control) = ControlledTcpStream::new(
            socket.with_observer(Some(observer.clone())),
            CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(io.local_addr().unwrap(), local);
        assert_eq!(io.peer_addr().unwrap(), remote);
        io.write_all(b"request").await.unwrap();
        io.shutdown().await.unwrap();
        let mut request = Vec::new();
        peer.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");
        peer.write_all(b"reverse reply").await.unwrap();
        peer.shutdown().await.unwrap();
        let mut reply = Vec::new();
        io.read_to_end(&mut reply).await.unwrap();
        assert_eq!(reply, b"reverse reply");
        assert_eq!(observer.boundaries.load(Ordering::Relaxed), 1);
        assert_eq!(observer.written.load(Ordering::Relaxed), 7);
        assert_eq!(observer.read.load(Ordering::Relaxed), 13);
        assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        // io still exists; the separate control cancelled that same byte view.
        assert!(io.write_all(b"late").await.is_err());
        assert!(control
            .wait_closed(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn controlled_tcp_pending_read_wakes_on_local_cancel_without_cancelling_parent_or_sibling() {
    timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = pair().await;
        let (sibling, mut sibling_peer) = pair().await;
        let parent = CancellationToken::new();
        let (mut io, mut control) = socket.into_controlled(parent.clone()).unwrap();
        let (mut sibling, mut sibling_control) = sibling.into_controlled(parent.clone()).unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let reader = tokio::spawn(async move {
            let mut bytes = [0; 1];
            let result = std::future::poll_fn(|cx| {
                let mut buffer = ReadBuf::new(&mut bytes);
                let result = Pin::new(&mut io).poll_read(cx, &mut buffer);
                if result.is_pending() {
                    entering.notify_one();
                }
                result
            })
            .await;
            (io, result)
        });
        entered.notified().await;
        assert!(!reader.is_finished());
        control.cancel(CloseReason::Shutdown);
        let (mut io, result) = reader.await.unwrap();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(!parent.is_cancelled());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock,
            "cancel alone is no native close"
        );
        sibling.write_all(b"still live").await.unwrap();
        let mut bytes = [0; 10];
        sibling_peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"still live");
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(
            io.read(&mut [0; 1]).await.unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert!(sibling_control
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn controlled_tcp_dropped_io_and_abandoned_or_expired_close_keep_owner_until_native_confirmation(
) {
    timeout(Duration::from_secs(3), async {
        let (socket, mut peer) = pair().await;
        let (io, mut control) = socket.into_controlled(CancellationToken::new()).unwrap();
        drop(io);
        {
            let closing = control.wait_closed(deadline());
            drop(closing);
        }
        let report = control.wait_closed(Instant::now()).await.unwrap();
        assert!(!report.is_complete());
        assert!(report.closed_io.is_empty());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let report = control.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn controlled_tcp_cancellation_wakes_a_real_backpressured_write_without_reporting_remote_delivery(
) {
    timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_recv_buffer_size(1024).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(8).unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(1024).unwrap();
        let (socket, peer) = tokio::join!(
            socket.connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let (mut io, mut control) = TokioSocket::new(socket.unwrap())
            .into_controlled(CancellationToken::new())
            .unwrap();
        let _peer = peer.unwrap().0; // Intentionally retain a real peer that does not read.
        let entered = Arc::new(tokio::sync::Notify::new());
        let entering = entered.clone();
        let writer = tokio::spawn(async move {
            let bytes = [7; 65536];
            let mut accepted = 0;
            let result = std::future::poll_fn(|cx| loop {
                match Pin::new(&mut io).poll_write(cx, &bytes) {
                    Poll::Ready(Ok(n)) => {
                        assert!(n > 0);
                        accepted += n;
                        assert!(
                            accepted < 64 * 1024 * 1024,
                            "actual peer window must backpressure bounded test writes"
                        );
                    }
                    Poll::Pending => {
                        entering.notify_one();
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => return Poll::Ready(Err::<(), _>(error)),
                }
            })
            .await;
            (io, accepted, result)
        });
        entered.notified().await;
        assert!(!writer.is_finished());
        control.cancel(CloseReason::Shutdown);
        let (_io, accepted, result) = writer.await.unwrap();
        assert!(accepted > 0);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::ConnectionAborted);
        assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
        // Accepted local bytes are deliberately not claimed as remote delivery.
    })
    .await
    .unwrap();
}
