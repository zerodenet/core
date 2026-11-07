//! A VMess empty chunk ends one logical direction, not the TLS/H2 carrier.
use super::*;
use std::{
    fmt,
    sync::{Arc, Mutex},
    task::{Wake, Waker},
};
#[derive(Default)]
pub(super) struct NativeShutdown {
    requested: bool,
    result: Option<Result<(), Arc<io::Error>>>,
    wake: Arc<WakeBoth>,
}
#[derive(Default)]
struct WakeBoth {
    rows: Mutex<[Option<Arc<Waker>>; 2]>,
}
impl WakeBoth {
    fn register(&self, reading: bool, waker: &Waker) {
        // RawWaker clone/drop and external wake never cross our metadata lock.
        let new = Arc::new(waker.clone());
        let old = { self.rows.lock().unwrap()[usize::from(!reading)].replace(new) };
        drop(old);
    }
    fn signal(&self) {
        let rows = {
            let rows = self.rows.lock().unwrap();
            [rows[0].clone(), rows[1].clone()]
        };
        for waker in rows.into_iter().flatten() {
            waker.wake_by_ref();
        }
    }
    fn clear(&self) {
        let rows = std::mem::take(&mut *self.rows.lock().unwrap());
        drop(rows);
    }
}
impl Wake for WakeBoth {
    fn wake(self: Arc<Self>) {
        self.signal();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
impl Drop for NativeShutdown {
    fn drop(&mut self) {
        self.wake.clear();
    }
}
#[derive(Debug)]
struct OriginalFailure(Arc<io::Error>);
impl fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> VmessAeadStream<S> {
    pub(super) fn poll_framed_read_end(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.native_shutdown.requested {
            return Poll::Ready(Ok(()));
        }
        self.poll_shutdown_pipeline(cx, true)
    }
    pub(super) fn poll_shutdown_encrypted(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.chunk_stream {
            return Pin::new(&mut self.inner).poll_shutdown(cx);
        }
        self.native_shutdown.requested = true;
        self.poll_shutdown_pipeline(cx, false)
    }
    fn poll_shutdown_work(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other.map(|result| result.map(|()| false)),
        }
        if !self.write_shutdown_sent {
            self.write_buf = self.writer.seal_chunk(&[]).map_err(protocol_error)?;
            self.write_pos = 0;
            self.write_shutdown_sent = true;
            match self.poll_flush_pending(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other.map(|result| result.map(|()| false)),
            }
        }
        if !self.write_shutdown_flushed {
            match Pin::new(&mut self.inner).poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other.map(|result| result.map(|()| false)),
            }
            self.write_shutdown_flushed = true;
        }
        if !matches!(self.read_state, ReadState::Eof) {
            return Poll::Ready(Ok(false));
        }
        Pin::new(&mut self.inner)
            .poll_shutdown(cx)
            .map(|result| result.map(|()| true))
    }
    fn poll_shutdown_pipeline(
        &mut self,
        cx: &mut Context<'_>,
        reading: bool,
    ) -> Poll<io::Result<()>> {
        if let Some(result) = &self.native_shutdown.result {
            return Poll::Ready(match result {
                Ok(()) => Ok(()),
                Err(source) => Err(io::Error::new(
                    source.kind(),
                    OriginalFailure(source.clone()),
                )),
            });
        }
        self.native_shutdown.wake.register(reading, cx.waker());
        let waker = Waker::from(self.native_shutdown.wake.clone());
        let mut actual = Context::from_waker(&waker);
        // All three stages (pending frame bytes, real flush, real shutdown)
        // remain in this same IO. No awaiter or direction bit owns progress.
        let result = match self.poll_shutdown_work(&mut actual) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(false)) => {
                self.native_shutdown.wake.clear();
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(result) => result.map(|_| ()).map_err(Arc::new),
        };
        self.native_shutdown.result = Some(result);
        self.native_shutdown.wake.signal();
        self.native_shutdown.wake.clear();
        match self.native_shutdown.result.as_ref().unwrap() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(source) => Poll::Ready(Err(io::Error::new(
                source.kind(),
                OriginalFailure(source.clone()),
            ))),
        }
    }
}
