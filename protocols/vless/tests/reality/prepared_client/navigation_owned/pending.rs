//! Observe the original socket's actual Pending read, never a sleep or PID.
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::oneshot,
};
pub(super) struct Observation {
    armed: AtomicBool,
    pending: Mutex<Option<oneshot::Sender<()>>>,
    reader: Mutex<Option<Waker>>,
}
impl Observation {
    pub(super) fn new() -> (Arc<Self>, oneshot::Receiver<()>) {
        let (send, receive) = oneshot::channel();
        (
            Arc::new(Self {
                armed: AtomicBool::new(false),
                pending: Mutex::new(Some(send)),
                reader: Mutex::new(None),
            }),
            receive,
        )
    }
    pub(super) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
        let wake = { self.reader.lock().unwrap().clone() };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
pub(super) struct ReadPending<S> {
    io: S,
    observation: Arc<Observation>,
}
impl<S> ReadPending<S> {
    pub(super) fn new(io: S, observation: Arc<Observation>) -> Self {
        Self { io, observation }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for ReadPending<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let result = Pin::new(&mut self.io).poll_read(cx, buffer);
        if result.is_pending() {
            let old = {
                self.observation
                    .reader
                    .lock()
                    .unwrap()
                    .replace(cx.waker().clone())
            };
            drop(old);
            if self.observation.armed.load(Ordering::Acquire) {
                let pending = { self.observation.pending.lock().unwrap().take() };
                if let Some(pending) = pending {
                    let _ = pending.send(());
                }
            }
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ReadPending<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
