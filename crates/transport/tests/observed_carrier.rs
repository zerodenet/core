use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::TokioSocket;
use zero_traits::IoObserver;
use zero_transport::{observed::ObservedStream, ClientStream, TcpRelayStream};

#[derive(Debug, Default)]
struct Counts {
    rx: AtomicU64,
    tx: AtomicU64,
    errors: AtomicU64,
}
impl IoObserver for Counts {
    fn received(&self, n: usize) {
        self.rx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn sent(&self, n: usize) {
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn dropped(&self) {}
}
struct Partial(TcpRelayStream);
impl AsyncRead for Partial {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for Partial {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = buf.len().min(2);
        Pin::new(&mut self.0).poll_write(cx, &buf[..n])
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for Partial {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl ClientStream for Partial {}
#[tokio::test]
async fn stream_and_socket_observation_are_live_and_include_partial_writes_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let echo = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 5];
        stream.read_exact(&mut bytes).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
    });
    let physical = Arc::new(Counts::default());
    let logical = Arc::new(Counts::default());
    let socket = TokioSocket::new(TcpStream::connect(address).await.unwrap())
        .with_observer(Some(physical.clone()));
    let mut stream = ObservedStream::new(Partial(TcpRelayStream::new(socket)), logical.clone());
    zero_traits::AsyncSocket::write_all(&mut stream, b"hello")
        .await
        .unwrap();
    assert_eq!(physical.tx.load(Ordering::Relaxed), 5);
    assert_eq!(logical.tx.load(Ordering::Relaxed), 5);
    let mut bytes = [0; 5];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"hello");
    assert_eq!(physical.rx.load(Ordering::Relaxed), 5);
    assert_eq!(logical.rx.load(Ordering::Relaxed), 5);
    assert_eq!(logical.errors.load(Ordering::Relaxed), 0);
    echo.await.unwrap();
}
