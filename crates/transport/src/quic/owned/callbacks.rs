//! External observation may panic while Quinn itself holds a protocol mutex.
//! Only those callbacks are caught. Their original payload resumes immediately
//! after the same actual driver poll has returned and released Quinn's locks.
use std::{
    any::Any,
    cell::RefCell,
    future::Future,
    io,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
mod observer;
pub(super) use observer::Observer;
type Payload = Box<dyn Any + Send>;
#[derive(Clone)]
struct Active {
    broker: Arc<Broker>,
    slot: Arc<Mutex<Option<Payload>>>,
}
thread_local! { static CURRENT: RefCell<Option<Active>> = const { RefCell::new(None) }; }
pub(super) struct Broker {
    failed: AtomicBool,
    active: AtomicUsize,
    limit: usize,
    suppressed: Mutex<Vec<Payload>>,
    coverage_lost: AtomicBool,
    retired_observers: Mutex<Vec<Arc<dyn zero_traits::IoObserver>>>,
}
impl Broker {
    pub(super) fn new(send_calls: usize) -> io::Result<Arc<Self>> {
        // Same-original simultaneous callbacks: actual whole TX call permits,
        // the unique RX, and its unique mutable native control. No new default.
        let limit = send_calls.checked_add(2).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "UDP callback bound overflow")
        })?;
        // Each old snapshot can contribute one callback panic and, at most,
        // one last-observer Drop panic before sticky fault prevents new installs.
        let panic_limit = limit.checked_mul(2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP callback panic bound overflow",
            )
        })?;
        let mut suppressed = Vec::new();
        suppressed
            .try_reserve_exact(panic_limit)
            .map_err(io::Error::other)?;
        let mut retired_observers = Vec::new();
        retired_observers
            .try_reserve_exact(limit)
            .map_err(io::Error::other)?;
        Ok(Arc::new(Self {
            failed: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            limit,
            suppressed: Mutex::new(suppressed),
            coverage_lost: AtomicBool::new(false),
            retired_observers: Mutex::new(retired_observers),
        }))
    }
    pub(super) fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
    pub(super) fn check(self: &Arc<Self>) -> io::Result<()> {
        let foreign = CURRENT.with(|current| {
            current
                .borrow()
                .as_ref()
                .is_some_and(|active| !Arc::ptr_eq(&active.broker, self))
        });
        if foreign {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata socket belongs to another actual driver owner",
            ))
        } else {
            Ok(())
        }
    }
    /// A sealed foreground user can synchronously make Quinn submit a reply
    /// (notably Incoming::accept/refuse). Resume external observation only
    /// after that original call has released its protocol locks. This is a
    /// user-call boundary, never a driver origin, task exit or failure ACK.
    pub(super) fn call<T>(self: &Arc<Self>, call: impl FnOnce() -> T) -> T {
        let slot = Arc::new(Mutex::new(None));
        let active = Active {
            broker: self.clone(),
            slot: slot.clone(),
        };
        let restore = Restore(CURRENT.with(|current| current.replace(Some(active))));
        let result = call();
        drop(restore);
        let payload = slot.lock().unwrap().take();
        if let Some(payload) = payload {
            resume_unwind(payload);
        }
        result
    }
    fn invoke(self: &Arc<Self>, callback: impl FnOnce()) {
        self.invoke_inner(callback, false);
    }
    fn invoke_inner(self: &Arc<Self>, callback: impl FnOnce(), dropping: bool) {
        if self.failed() && !dropping {
            self.coverage_lost.store(true, Ordering::Release);
            return;
        }
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if active >= self.limit && !dropping {
                // This cannot arise from the declared send/RX/control graph.
                // Refuse another external callback rather than allocate an
                // unbounded panic queue or quietly claim complete observation.
                self.coverage_lost.store(true, Ordering::Release);
                self.failed.store(true, Ordering::Release);
                return;
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }
        let invocation = Invocation(self.clone());
        if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
            self.failed.store(true, Ordering::Release);
            let context = CURRENT.with(|current| current.borrow().clone());
            if let Some(context) = context.filter(|active| Arc::ptr_eq(&active.broker, self)) {
                let leftover = {
                    let mut slot = context.slot.lock().unwrap();
                    if slot.is_none() {
                        *slot = Some(payload);
                        None
                    } else {
                        Some(payload)
                    }
                };
                if let Some(payload) = leftover {
                    // First original payload wins. Already-entered ancestor
                    // callbacks may also panic, but all payloads remain owned;
                    // none is dropped while the primary panic unwinds Quinn.
                    self.suppressed.lock().unwrap().push(payload);
                }
            } else {
                drop(invocation);
                resume_unwind(payload);
            }
        }
    }
    fn retire_observer(self: &Arc<Self>, original: Option<Arc<dyn zero_traits::IoObserver>>) {
        let Some(original) = original else { return };
        if self.failed() {
            // A snapshot may be the final observer Arc after native close.
            // Keep it with the persistent owner instead of running arbitrary
            // Drop during the primary driver unwind. The same send/RX/control
            // graph bounds old snapshots, and no observer installation is
            // accepted after fault. This is not a close or failure ACK.
            self.retired_observers.lock().unwrap().push(original);
        } else {
            // If another task faults between the check and this call, still
            // catch this last Drop instead of dropping its closure uncaught.
            self.invoke_inner(|| drop(original), true);
        }
    }
    pub(super) fn take_suppressed(&self) -> Option<Payload> {
        // Original payload ownership is transferred without running user Drop
        // or resuming a panic under either the owner or native/Quinn locks.
        self.suppressed.lock().unwrap().pop()
    }
    pub(super) fn suppressed_count(&self) -> usize {
        self.suppressed.lock().unwrap().len()
    }
    pub(super) fn coverage_lost(&self) -> bool {
        self.coverage_lost.load(Ordering::Acquire)
    }
}
struct Invocation(Arc<Broker>);
impl Drop for Invocation {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Restore(Option<Active>);
impl Drop for Restore {
    fn drop(&mut self) {
        let old = CURRENT.with(|current| current.replace(self.0.take()));
        drop(old);
    }
}
pub(super) struct Boundary<O> {
    future: Pin<Box<dyn Future<Output = O> + Send>>,
    broker: Arc<Broker>,
    slot: Arc<Mutex<Option<Payload>>>,
}
impl<O> Boundary<O> {
    pub(super) fn new(
        future: Pin<Box<dyn Future<Output = O> + Send>>,
        broker: Arc<Broker>,
    ) -> Self {
        Self {
            future,
            broker,
            slot: Arc::new(Mutex::new(None)),
        }
    }
}
impl<O> Future for Boundary<O> {
    type Output = O;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let active = Active {
            broker: this.broker.clone(),
            slot: this.slot.clone(),
        };
        let restore = Restore(CURRENT.with(|current| current.replace(Some(active))));
        let result = this.future.as_mut().poll(cx);
        drop(restore);
        let payload = this.slot.lock().unwrap().take();
        if let Some(payload) = payload {
            resume_unwind(payload);
        }
        result
    }
}
