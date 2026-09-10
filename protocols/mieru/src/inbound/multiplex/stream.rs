//! Bounded logical byte streams; connection encryption stays in the driver.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    sync::{mpsc, oneshot},
};
use zero_traits::AsyncSocket;

use super::writer::Command;

#[derive(Default)]
pub(crate) struct Status(Mutex<State>);
#[derive(Default)]
struct State {
    opened: bool,
    closed: bool,
    error: Option<io::ErrorKind>,
    writer: Option<Waker>,
}
impl Status {
    pub(crate) fn mark_open(&self) {
        let mut state = self.0.lock().unwrap();
        state.opened = true;
        if let Some(waker) = state.writer.take() {
            waker.wake();
        }
    }
    pub(crate) async fn wait_open(&self) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            let mut state = self.0.lock().unwrap();
            if state.closed {
                return Poll::Ready(Err(state.error.unwrap_or(io::ErrorKind::BrokenPipe).into()));
            }
            if state.opened {
                return Poll::Ready(Ok(()));
            }
            state.writer = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    pub(crate) fn close(&self, error: Option<io::ErrorKind>) {
        let mut state = self.0.lock().unwrap();
        if !state.closed {
            state.closed = true;
            state.error = error;
        }
        if let Some(w) = state.writer.take() {
            w.wake();
        }
    }
    pub(crate) fn closed(&self) -> bool {
        self.0.lock().unwrap().closed
    }
    fn read_end(&self) -> io::Result<()> {
        match self.0.lock().unwrap().error {
            Some(kind) => Err(io::Error::new(kind, "mieru logical session failed")),
            None => Ok(()),
        }
    }
    fn writable(&self, cx: &Context<'_>) -> io::Result<()> {
        let mut state = self.0.lock().unwrap();
        state.writer = Some(cx.waker().clone());
        if state.closed {
            Err(io::Error::new(
                state.error.unwrap_or(io::ErrorKind::BrokenPipe),
                "mieru session closed",
            ))
        } else {
            Ok(())
        }
    }
}

type Reservation = Pin<
    Box<
        dyn Future<Output = Result<mpsc::OwnedPermit<Command>, mpsc::error::SendError<()>>>
            + Send
            + Sync,
    >,
>;

pub struct MieruLogicalStream {
    pub(crate) id: u32,
    pub(crate) status: Arc<Status>,
    pub(crate) incoming: mpsc::Receiver<Vec<u8>>,
    pub(crate) outgoing: mpsc::Sender<Command>,
    pub(crate) dropped: mpsc::UnboundedSender<(u32, Arc<Status>)>,
    reserve: Option<Reservation>,
    barrier: Option<oneshot::Receiver<io::Result<()>>>,
    buffered: Vec<u8>,
    offset: usize,
    shutdown: bool,
    retired: bool,
    keepalive: Option<Arc<dyn Send + Sync>>,
}
impl MieruLogicalStream {
    pub(crate) fn new(
        id: u32,
        status: Arc<Status>,
        incoming: mpsc::Receiver<Vec<u8>>,
        outgoing: mpsc::Sender<Command>,
        dropped: mpsc::UnboundedSender<(u32, Arc<Status>)>,
    ) -> Self {
        Self {
            id,
            status,
            incoming,
            outgoing,
            dropped,
            reserve: None,
            barrier: None,
            buffered: Vec::new(),
            offset: 0,
            shutdown: false,
            retired: false,
            keepalive: None,
        }
    }
    pub(crate) fn keep_alive(&mut self, value: Arc<dyn Send + Sync>) {
        self.keepalive = Some(value);
    }
    fn retire(&mut self) {
        if !self.retired {
            self.retired = true;
            let _ = self.dropped.send((self.id, self.status.clone()));
        }
    }
    fn permit(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<mpsc::OwnedPermit<Command>>> {
        let future = self
            .reserve
            .get_or_insert_with(|| Box::pin(self.outgoing.clone().reserve_owned()));
        let result = std::task::ready!(future.as_mut().poll(cx));
        self.reserve = None;
        Poll::Ready(
            result.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "mieru underlay closed")),
        )
    }
    fn poll_barrier(&mut self, cx: &mut Context<'_>, close: bool) -> Poll<io::Result<()>> {
        if self.barrier.is_none() {
            if self.shutdown {
                return Poll::Ready(Ok(()));
            }
            self.status.writable(cx)?;
            let permit = std::task::ready!(self.permit(cx))?;
            let (tx, rx) = oneshot::channel();
            permit.send(Command::Barrier {
                id: self.id,
                close,
                status: self.status.clone(),
                done: tx,
            });
            self.barrier = Some(rx);
            self.shutdown = close;
        }
        let result = std::task::ready!(Pin::new(self.barrier.as_mut().unwrap()).poll(cx));
        self.barrier = None;
        if self.shutdown {
            self.retire();
        }
        Poll::Ready(result.unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "mieru writer stopped",
            ))
        }))
    }
}
impl Drop for MieruLogicalStream {
    fn drop(&mut self) {
        self.retire();
    }
}
impl AsyncRead for MieruLogicalStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while self.offset == self.buffered.len() {
            match std::task::ready!(self.incoming.poll_recv(cx)) {
                Some(data) => {
                    self.buffered = data;
                    self.offset = 0;
                }
                None => return Poll::Ready(self.status.read_end()),
            }
        }
        let n = buf.remaining().min(self.buffered.len() - self.offset);
        buf.put_slice(&self.buffered[self.offset..self.offset + n]);
        self.offset += n;
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for MieruLogicalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.barrier.is_some() {
            std::task::ready!(self.poll_barrier(cx, false))?;
        }
        self.status.writable(cx)?;
        if self.shutdown {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let permit = std::task::ready!(self.permit(cx))?;
        let n = buf.len().min(crate::segment::MAX_FRAGMENT);
        permit.send(Command::Data {
            id: self.id,
            payload: buf[..n].to_vec(),
            status: self.status.clone(),
        });
        Poll::Ready(Ok(n))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_barrier(cx, false)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_barrier(cx, false))?;
        self.poll_barrier(cx, true)
    }
}
impl AsyncSocket for MieruLogicalStream {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
