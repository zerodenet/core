use std::{
    io,
    sync::Mutex,
    task::{Context, Poll, Waker},
};
#[derive(Default)]
pub(crate) struct Status(
    Mutex<State>,
    #[cfg(feature = "runtime")] tokio::sync::Notify,
);
#[derive(Default)]
struct State {
    opened: bool,
    closed: bool,
    error: Option<io::ErrorKind>,
    #[cfg(feature = "runtime")]
    removed: bool,
    writer: Option<Waker>,
}
impl Status {
    pub(crate) fn mark_open(&self) {
        let mut state = self.0.lock().unwrap();
        state.opened = true;
        let wake = state.writer.take();
        drop(state);
        if let Some(waker) = wake {
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
        let wake = state.writer.take();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
    }
    pub(crate) fn closed(&self) -> bool {
        self.0.lock().unwrap().closed
    }
    pub(super) fn read_end(&self) -> io::Result<()> {
        match self.0.lock().unwrap().error {
            Some(kind) => Err(io::Error::new(kind, "mieru logical session failed")),
            None => Ok(()),
        }
    }
    pub(super) fn writable(&self, cx: &Context<'_>) -> io::Result<()> {
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

#[cfg(feature = "runtime")]
impl Status {
    pub(crate) fn removed(&self) {
        self.0.lock().unwrap().removed = true;
        self.1.notify_waiters();
    }
    pub(crate) fn is_removed(&self) -> bool {
        self.0.lock().unwrap().removed
    }
    pub(crate) async fn wait_removed(&self) {
        loop {
            let changed = self.1.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_removed() {
                return;
            }
            changed.await;
        }
    }
}
