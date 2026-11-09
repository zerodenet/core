use super::*;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct Probe {
    raw: tokio::io::DuplexStream,
    pending: Arc<AtomicBool>,
    original: Arc<Mutex<Option<Waker>>>,
}
impl AsyncRead for Probe {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_read(cx, out)
    }
}
impl AsyncWrite for Probe {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.raw).poll_write(cx, bytes);
        if result.is_pending() {
            self.pending.store(true, Ordering::Release);
            *self.original.lock().unwrap() = Some(cx.waker().clone());
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.raw).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn actual_pending_write_cancellation_frees_real_backing_but_last_native_waker_header_stays_paid(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (book, metadata) = roots();
        let before = metadata.snapshot();
        let masks =
            PreparedMasks::new_owned(&[fragmented()], book.clone(), metadata.clone()).unwrap();
        let prepared = masks.prepare_owned(book.clone(), metadata.clone()).unwrap();
        drop(masks);
        let (raw, peer) = tokio::io::duplex(1);
        let pending = Arc::new(AtomicBool::new(false));
        let original = Arc::new(Mutex::new(None));
        let raw = Probe {
            raw,
            pending: pending.clone(),
            original: original.clone(),
        };
        let mut stream = prepared
            .wrap(TcpRelayStream::new(raw), false)
            .await
            .unwrap();
        stream.write_all(&vec![0x45; 65536]).await.unwrap();
        std::future::poll_fn(|cx| {
            assert!(Pin::new(&mut stream).poll_flush(cx).is_pending());
            assert!(
                pending.load(Ordering::Acquire),
                "actual underlying write was observed Pending, beyond the accepted byte view"
            );
            Poll::Ready(())
        })
        .await;
        let held = original.lock().unwrap().take().unwrap();
        assert!(book.used_bytes() > 0);
        drop(stream);
        drop(peer);
        assert_eq!(book.used_bytes(), 0);
        assert!(
            metadata.snapshot().occupied.bytes() > before.occupied.bytes(),
            "the actual native poller's held Waker still owns its original header"
        );
        drop(held);
        assert_eq!(metadata.snapshot(), before);
    })
    .await
    .unwrap();
}
