//! Drive the same client connection in the real application caller, on every
//! read, write, flush and half-close. No detached task or application queue.
use super::owned::{H2ClientDriver, OwnedH2Client, PreparedH2Client};
use super::*;
use futures_util::task::{waker_ref, ArcWake, AtomicWaker};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
struct WakeBoth {
    reader: AtomicWaker,
    writer: AtomicWaker,
    ready: AtomicBool,
    polling: AtomicBool,
}
impl Default for WakeBoth {
    fn default() -> Self {
        Self {
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            ready: AtomicBool::new(true),
            polling: AtomicBool::new(false),
        }
    }
}
impl ArcWake for WakeBoth {
    fn wake_by_ref(this: &Arc<Self>) {
        // One pending original-driver turn already has its application wake.
        // Coalesce duplicate stream/socket notifications until that same turn
        // consumes readiness; clearing before poll preserves concurrent wakes.
        if this.ready.swap(true, Ordering::AcqRel) {
            return;
        }
        if !this.polling.load(Ordering::Acquire) {
            this.reader.wake();
            this.writer.wake();
        }
    }
}
/// A synchronous turn may consume its own DATA/WINDOW_UPDATE notification
/// before returning. Defer that notification until the turn exits, including
/// unwinding; only still-unconsumed readiness needs another application poll.
struct Polling<'a>(&'a WakeBoth);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.polling.store(false, Ordering::Release);
        if self.0.ready.load(Ordering::Acquire) {
            self.0.reader.wake();
            self.0.writer.wake();
        }
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
    fn drive(driver: &mut Option<H2ClientDriver>, wake: &Arc<WakeBoth>) {
        let Some(future) = driver.as_mut() else {
            return;
        };
        // Poll only the original driver's initial turn or actual wake. An
        // application capacity-Pending poll supplies no new native readiness.
        // Clear before polling so an inline wake preserves the next turn.
        if !wake.ready.swap(false, Ordering::AcqRel) {
            return;
        }
        let driver_waker = waker_ref(wake);
        let mut cx = Context::from_waker(&driver_waker);
        if future.as_mut().poll(&mut cx).is_ready() {
            *driver = None;
            wake.reader.wake();
            wake.writer.wake();
        }
    }
    fn poll<T>(
        &mut self,
        cx: &mut Context<'_>,
        reading: bool,
        operation: impl FnOnce(Pin<&mut OwnedH2Client>, &mut Context<'_>) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        let Self {
            stream,
            driver,
            wake,
        } = self;
        wake.polling.store(true, Ordering::Release);
        let _polling = Polling(wake);
        Self::drive(driver, wake);
        // Stream-capacity/response notifications belong to this same inline
        // turn as native-driver readiness. The driver may grant capacity while
        // it is already being polled here; waking the application directly in
        // that case schedules an extra turn even when this operation consumes
        // the grant synchronously. Keep the notification in WakeBoth and only
        // forward a still-pending turn to the actual reader/writer below.
        let operation_waker = waker_ref(wake);
        let mut operation_context = Context::from_waker(&operation_waker);
        let result = operation(Pin::new(stream), &mut operation_context);
        // Only a pending application operation is owed a future wake. Install
        // it before the post-drive turn; synchronous driver notifications stay
        // retained by Polling until after this registration. A successful
        // one-byte DATA write needs no redundant AtomicWaker registration.
        if result.is_pending() {
            if reading {
                wake.reader.register(cx.waker());
            } else {
                wake.writer.register(cx.waker());
            }
        }
        // The same original is also driven after application enqueue/credit
        // return. Pending capacity alone cannot manufacture readiness.
        Self::drive(driver, wake);
        result
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
        this.poll(cx, true, |stream, cx| stream.poll_read(cx, out))
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
        this.poll(cx, false, |stream, cx| stream.poll_write(cx, bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.poll(cx, false, |stream, cx| stream.poll_flush(cx))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive inline H2 carrier");
        this.poll(cx, false, |stream, cx| stream.poll_shutdown(cx))
    }
}
