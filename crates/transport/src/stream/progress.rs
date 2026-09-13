use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
    task::{Context, Poll},
};
#[derive(Default)]
pub(crate) struct Progress {
    accepted: AtomicU64,
    committed: AtomicU64,
    finished: AtomicBool,
    error: Mutex<Option<String>>,
    waker: futures_util::task::AtomicWaker,
}
impl Progress {
    pub fn accept(&self, count: usize) {
        self.accepted.fetch_add(count as u64, Ordering::Release);
    }
    pub fn commit(&self, count: usize) {
        self.committed.fetch_add(count as u64, Ordering::Release);
        self.waker.wake();
    }
    pub fn finish(&self) {
        self.finished.store(true, Ordering::Release);
        self.waker.wake();
    }
    pub fn fail(&self, error: impl std::fmt::Display) {
        *self.error.lock().unwrap() = Some(error.to_string());
        self.waker.wake();
    }
    pub fn check(&self) -> io::Result<()> {
        match self.error.lock().unwrap().as_ref() {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
    pub fn poll(&self, cx: &mut Context<'_>, shutdown: bool) -> Poll<io::Result<()>> {
        self.waker.register(cx.waker());
        self.check()?;
        if self.committed.load(Ordering::Acquire) >= self.accepted.load(Ordering::Acquire)
            && (!shutdown || self.finished.load(Ordering::Acquire))
        {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
