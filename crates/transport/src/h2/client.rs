//! Drive the same client connection in the real application caller, on every
//! read, write, flush and half-close. No detached task or application queue.
use super::owned::{H2ClientDriver, OwnedH2Client, PreparedH2Client};
use super::*;
use futures_util::task::{ArcWake, AtomicWaker, waker_ref};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct WakeBoth {
    reader: AtomicWaker,
    writer: AtomicWaker,
}
impl ArcWake for WakeBoth {
    fn wake_by_ref(this: &Arc<Self>) {
        this.reader.wake();
        this.writer.wake();
    }
}
pub(super) struct Inline(Mutex<State>);
struct State {
    stream: OwnedH2Client,
    driver: Option<H2ClientDriver>,
    wake: Arc<WakeBoth>,
}
impl Inline {
    pub(super) fn new(prepared: PreparedH2Client) -> Self {
        Self(Mutex::new(State {
            stream: prepared.stream,
            driver: Some(prepared.driver),
            wake: Arc::default(),
        }))
    }
}
impl State {
    fn drive(&mut self) {
        let Some(driver) = &mut self.driver else {
            return;
        };
        let wake = waker_ref(&self.wake);
        let mut cx = Context::from_waker(&wake);
        if driver.as_mut().poll(&mut cx).is_ready() {
            self.driver = None;
            self.wake.reader.wake();
            self.wake.writer.wake();
        }
    }
}
impl AsyncRead for Inline {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.wake.reader.register(cx.waker());
        this.drive();
        let result = Pin::new(&mut this.stream).poll_read(cx, out);
        // Read releases real H2 credit; progress its outbound WINDOW_UPDATE too.
        this.drive();
        result
    }
}
impl AsyncWrite for Inline {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.wake.writer.register(cx.waker());
        this.drive();
        let result = Pin::new(&mut this.stream).poll_write(cx, bytes);
        // This includes capacity-Pending writes: SETTINGS/WINDOW_UPDATE and the
        // original socket remain driven even when no application read exists.
        this.drive();
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.wake.writer.register(cx.waker());
        this.drive();
        let result = Pin::new(&mut this.stream).poll_flush(cx);
        this.drive();
        result
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.wake.writer.register(cx.waker());
        this.drive();
        let result = Pin::new(&mut this.stream).poll_shutdown(cx);
        this.drive();
        result
    }
}
