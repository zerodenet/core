use super::epoch::ACTUAL_TASK;
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{contract::scope::TaskJoinWitness, ControlledTcpStream};
#[derive(Default)]
pub(super) struct Custody {
    views: AtomicUsize,
    users: Mutex<Vec<TaskJoinWitness>>,
}
impl Custody {
    pub(super) fn add(&self, witness: TaskJoinWitness) {
        let mut users = self.users.lock().unwrap();
        if !users.iter().any(|old| old.same_registration(&witness)) {
            users.push(witness);
        }
    }
    fn observe(&self) -> io::Result<()> {
        ACTUAL_TASK
            .try_with(|witness| self.add(witness.clone()))
            .map_err(|_| io::Error::other("event TCP I/O requires its original tracked task"))
    }
    pub(super) fn returned_and_joined(&self) -> bool {
        self.views.load(Ordering::Acquire) == 0
            && self
                .users
                .lock()
                .unwrap()
                .iter()
                .all(TaskJoinWitness::is_joined)
    }
}
pub(super) struct EventStream {
    inner: ControlledTcpStream,
    custody: Arc<Custody>,
}
impl EventStream {
    pub(super) fn new(inner: ControlledTcpStream, custody: Arc<Custody>) -> Self {
        custody.views.fetch_add(1, Ordering::AcqRel);
        Self { inner, custody }
    }
}
impl Drop for EventStream {
    fn drop(&mut self) {
        self.custody.views.fetch_sub(1, Ordering::AcqRel);
    }
}
impl AsyncRead for EventStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.custody.observe() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for EventStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.custody.observe() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.custody.observe() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.custody.observe() {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
