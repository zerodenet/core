//! One native writer registration fans out to explicitly bounded pollers.
//! External wakers are never cloned, dropped or invoked with I/O/registry locks.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
#[derive(Default)]
struct Slot(Mutex<Option<Waker>>);
impl Slot {
    fn install(&self, waker: &Waker) {
        let new = waker.clone(); // External RawWaker code precedes our lock.
        let old = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(new);
        drop(old);
    }
    fn clear(&self) {
        let old = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(old);
    }
    fn wake(&self) {
        let waker = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
struct Slots {
    next: u64,
    writes: usize,
    slots: BTreeMap<u64, Weak<Slot>>,
}
pub(super) struct Broker {
    slots: Mutex<Slots>,
    write_limit: NonZeroUsize,
    guarded: AtomicUsize,
    pending: AtomicBool,
    flushing: AtomicBool,
}
impl Broker {
    pub(super) fn new(write_limit: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            slots: Mutex::new(Slots {
                next: 0,
                writes: 0,
                slots: BTreeMap::new(),
            }),
            write_limit,
            guarded: AtomicUsize::new(0),
            pending: AtomicBool::new(false),
            flushing: AtomicBool::new(false),
        })
    }
    pub(super) fn register(self: &Arc<Self>) -> std_io::Result<Registration> {
        self.register_inner(true)
    }
    pub(super) fn receiver(self: &Arc<Self>) -> std_io::Result<Registration> {
        self.register_inner(false)
    }
    fn register_inner(self: &Arc<Self>, write: bool) -> std_io::Result<Registration> {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if write && slots.writes >= self.write_limit.get() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::WouldBlock,
                "UDP write poller capacity exhausted",
            ));
        }
        let next = slots
            .next
            .checked_add(1)
            .ok_or_else(|| std_io::Error::other("UDP poller identity exhausted"))?;
        let id = slots.next;
        let slot = Arc::new(Slot::default());
        slots.slots.insert(id, Arc::downgrade(&slot));
        slots.next = next;
        slots.writes += usize::from(write);
        Ok(Registration {
            broker: self.clone(),
            id,
            write,
            slot,
            cancelled: None,
        })
    }
    // Declare this before native guards so it is released after those guards.
    pub(super) fn defer(self: &Arc<Self>) -> Deferral {
        self.guarded.fetch_add(1, Ordering::AcqRel);
        Deferral(self.clone())
    }
    pub(super) fn signal(&self) {
        self.pending.store(true, Ordering::Release);
        self.flush();
    }
    fn flush(&self) {
        loop {
            if self.guarded.load(Ordering::Acquire) != 0
                || self
                    .flushing
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                return;
            }
            let flushing = FlushGuard(&self.flushing);
            // A callback may synchronously revoke/re-poll this same owner.
            // Nested signals only mark pending, never recursively call users.
            while self.guarded.load(Ordering::Acquire) == 0
                && self.pending.swap(false, Ordering::AcqRel)
            {
                let slots: Vec<_> = self
                    .slots
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .slots
                    .values()
                    .filter_map(Weak::upgrade)
                    .collect();
                for slot in slots {
                    slot.wake();
                }
            }
            drop(flushing);
            if self.guarded.load(Ordering::Acquire) != 0 || !self.pending.load(Ordering::Acquire) {
                return;
            }
        }
    }
}
struct FlushGuard<'a>(&'a AtomicBool);
impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl Wake for Broker {
    fn wake(self: Arc<Self>) {
        self.signal();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
pub(super) struct Deferral(Arc<Broker>);
impl Drop for Deferral {
    fn drop(&mut self) {
        if self.0.guarded.fetch_sub(1, Ordering::AcqRel) == 1 && !std::thread::panicking() {
            self.0.flush();
        }
    }
}
pub(super) struct Registration {
    broker: Arc<Broker>,
    id: u64,
    write: bool,
    slot: Arc<Slot>,
    cancelled: Option<Pin<Box<WaitForCancellationFutureOwned>>>,
}
impl Registration {
    /// Caller waker registration happens before acquiring the native gate.
    pub(super) fn prepare(&mut self, cx: &Context<'_>, cancel: &CancellationToken) -> bool {
        if cancel.is_cancelled() {
            self.slot.clear();
            return true;
        }
        self.slot.install(cx.waker());
        let cancelled = self
            .cancelled
            .get_or_insert_with(|| Box::pin(cancel.clone().cancelled_owned()));
        // Cancellation uses only an internal waker too. Its synchronous wake
        // joins the same fanout/deferral discipline as native readiness.
        let waker = Waker::from(self.broker.clone());
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    }
    pub(super) fn waker(&self) -> Waker {
        Waker::from(self.broker.clone())
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        // Return capacity after caller-waker storage is actually released.
        // Its external Drop may reenter admission, so retain our slot until then.
        self.slot.clear();
        {
            let mut slots = self
                .broker
                .slots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            slots.slots.remove(&self.id);
            slots.writes -= usize::from(self.write);
        }
        // The stored caller waker and cancellation future drop after this lock.
    }
}
pub struct DatagramWritePoller {
    pub(super) shared: Arc<Shared>,
    pub(super) registration: Registration,
    pub(super) _view: ViewToken,
}
impl DatagramWritePoller {
    pub fn poll_writable(&mut self, cx: &mut Context<'_>) -> Poll<std_io::Result<()>> {
        if self
            .registration
            .prepare(cx, &self.shared.socket.entry.retired)
        {
            return Poll::Ready(self.shared.check());
        }
        let waker = self.registration.waker();
        let mut internal = Context::from_waker(&waker);
        let _defer = self.shared.pollers.defer();
        let native = self.shared.socket.entry.lock();
        self.shared.check()?;
        match &*native {
            NativeHandleState::Live(socket) => socket.inner.poll_send_ready(&mut internal),
            other => Poll::Ready(Err(super::super::io::unavailable(other))),
        }
    }
}
