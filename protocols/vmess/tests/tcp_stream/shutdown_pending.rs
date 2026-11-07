//! Cancel the awaiter while the original carrier really cannot send shutdown bytes.
use super::*;
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
struct Tail<S> {
    inner: S,
    bytes: Vec<u8>,
    position: usize,
    written: Arc<AtomicUsize>,
    tail_on_flush: bool,
    enabled: Arc<AtomicBool>,
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync> AsyncSocket for Tail<S> {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(&mut self.inner, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(&mut self.inner, bytes).await?;
        AsyncWriteExt::flush(&mut self.inner).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for Tail<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Tail<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.tail_on_flush && self.enabled.load(Ordering::Acquire) {
            match self.as_mut().poll_tail(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.tail_on_flush {
            match self.as_mut().poll_tail(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl<S: AsyncWrite + Unpin> Tail<S> {
    fn poll_tail(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.position < this.bytes.len() {
            let n = match Pin::new(&mut this.inner).poll_write(cx, &this.bytes[this.position..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(n)) => n,
            };
            this.position += n;
            this.written.store(this.position, Ordering::Release);
        }
        Poll::Ready(Ok(()))
    }
}
async fn cancel_pending<F: Future<Output = io::Result<()>>>(
    future: F,
    ready: tokio::sync::oneshot::Receiver<()>,
    written: &AtomicUsize,
) {
    let mut future = Box::pin(future);
    tokio::select! {biased;result=&mut future=>panic!("shutdown unexpectedly completed before real peer drain: {result:?}"),result=ready=>result.unwrap()}
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let bytes = written.load(Ordering::Acquire);
    assert!(
        bytes > 0 && bytes < 2048,
        "genuine bounded carrier write is pending: {bytes}/2048"
    );
    // Drop this actual awaiter, not the stored original transport operation.
}
#[tokio::test]
async fn a_cancelled_shutdown_awaiter_can_be_replaced_by_either_live_vmess_direction() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        for (cancel_reader, tail_on_flush) in [(false, false), (true, false), (false, true)] {
            let (client, server) = tokio::io::duplex(64);
            let (ready, receiver) = tokio::sync::oneshot::channel();
            let (release, drain) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let profile = VmessInboundProfile::from_config_users([(
                    "11111111-2222-3333-4444-555555555555".to_owned(),
                    "aes-128-gcm".to_owned(),
                    None::<String>,
                    None::<u64>,
                    None::<u64>,
                    None::<u32>,
                    None::<u64>,
                    None::<u64>,
                )])
                .unwrap();
                let (_, mut stream) = profile
                    .accept_tcp_stream(VmessInbound, TestSocket(server))
                    .await
                    .unwrap();
                if !cancel_reader {
                    AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                }
                let mut upload = Vec::new();
                AsyncReadExt::read_to_end(&mut stream, &mut upload)
                    .await
                    .unwrap();
                assert_eq!(upload, b"genuine upload before framed end");
                if cancel_reader {
                    AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                }
                let mut original = stream.into_inner();
                ready.send(()).unwrap();
                drain.await.unwrap();
                let mut tail = Vec::new();
                AsyncReadExt::read_to_end(&mut original, &mut tail)
                    .await
                    .unwrap();
                assert!(
                    tail == vec![91; 2048],
                    "actual remaining native shutdown bytes={}/2048",
                    tail.len()
                );
            });
            let written = Arc::new(AtomicUsize::new(0));
            let enabled = Arc::new(AtomicBool::new(false));
            let request = PreparedVmessOutboundRequestBundle::from_config(
                "11111111-2222-3333-4444-555555555555",
                "aes-128-gcm",
                None,
            )
            .unwrap();
            let target = Session::new(
                0,
                Address::Ipv4([127, 0, 0, 1]),
                443,
                Network::Tcp,
                ProtocolType::new("vmess"),
            );
            let (mut stream, _) = request
                .establish_tcp_outbound_stream(
                    Tail {
                        inner: client,
                        bytes: vec![91; 2048],
                        position: 0,
                        written: written.clone(),
                        tail_on_flush,
                        enabled: enabled.clone(),
                    },
                    &target,
                )
                .await
                .unwrap();
            AsyncWriteExt::write_all(&mut stream, b"genuine upload before framed end")
                .await
                .unwrap();
            let mut byte = [0];
            if cancel_reader {
                AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                cancel_pending(
                    async {
                        let n = AsyncReadExt::read(&mut stream, &mut byte).await?;
                        assert_eq!(n, 0);
                        Ok(())
                    },
                    receiver,
                    &written,
                )
                .await;
                let mut finish = Box::pin(AsyncWriteExt::shutdown(&mut stream));
                poll_fn(|cx| {
                    assert!(finish.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                release.send(()).unwrap();
                finish.await.unwrap();
            } else {
                assert_eq!(AsyncReadExt::read(&mut stream, &mut byte).await.unwrap(), 0);
                enabled.store(true, Ordering::Release);
                cancel_pending(AsyncWriteExt::shutdown(&mut stream), receiver, &written).await;
                let mut finish = Box::pin(AsyncReadExt::read(&mut stream, &mut byte));
                poll_fn(|cx| {
                    assert!(finish.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                release.send(()).unwrap();
                assert_eq!(finish.await.unwrap(), 0);
            }
            assert_eq!(written.load(Ordering::Acquire), 2048);
            peer.await.unwrap();
        }
    })
    .await
    .expect("original eight-second actual pending shutdown cancellation deadline");
}
