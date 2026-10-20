//! Failure of one real H2 request must retire other request futures with the job.
use super::*;
use std::{
    pin::Pin,
    sync::Mutex as StdMutex,
    task::{Context, Poll},
};
use tokio::io::{AsyncReadExt, ReadBuf};

#[derive(Clone)]
struct Peer(Arc<StdMutex<tokio::net::TcpStream>>);
impl AsyncRead for Peer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for Peer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0.lock().unwrap()).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn two_real_pending_workers_retire_when_one_h2_request_fails_with_original_reset_source() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        let mut peer = Peer(Arc::new(StdMutex::new(peer)));
        let (_, mut job) = prepare(client, "failure.test".into(), Plan {
            path: "/".into(), ranges: [(0, 0), (2, 2), (1, 1), (0, 0), (0, 0)],
        }).unwrap().into_parts();
        let handshake = h2::server::handshake::<_, Bytes>(peer.clone());
        tokio::pin!(handshake);
        let mut server = tokio::select! {
            result = &mut handshake => result.unwrap(),
            result = &mut job => panic!("job finished before actual H2 handshake: {result:?}"),
        };
        let mut first = tokio::select! {
            request = server.accept() => request.unwrap().unwrap(),
            result = &mut job => panic!("job finished before first request: {result:?}"),
        };
        first.1.send_response(http::Response::new(()), true).unwrap();
        let mut first_worker = tokio::select! {
            request = server.accept() => request.unwrap().unwrap(),
            result = &mut job => panic!("job finished before first worker: {result:?}"),
        };
        let second_worker = tokio::select! {
            request = server.accept() => request.unwrap().unwrap(),
            result = &mut job => panic!("job finished before second concurrent worker: {result:?}"),
        };
        // Both workers issued real requests before either response was supplied.
        // Leave the second response pending while resetting the first.
        first_worker.1.send_reset(h2::Reason::REFUSED_STREAM);
        let result = tokio::select! { biased;
            result = &mut job => result,
            request = server.accept() => panic!("unexpected request after two workers: {}", request.is_some()),
        };
        let error = result.unwrap_err();
        let source = error.get_ref().unwrap().downcast_ref::<h2::Error>().unwrap();
        assert_eq!(source.reason(), Some(h2::Reason::REFUSED_STREAM));
        drop(job);
        drop(second_worker);
        drop(server);
        // Recover the original peer socket after its H2 driver was dropped;
        // EOF here proves that no detached worker retained the client carrier.
        let mut tail = Vec::new();
        peer.read_to_end(&mut tail).await.unwrap();
    }).await.unwrap();
}
