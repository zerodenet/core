//! One actual Windows pipe, its original worker and unique byte-stream view.
//! A notification is readiness only; CloseHandle follows actual std/Scope joins.
use crate::contract::{
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar, WeakTaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};
use std::{
    collections::VecDeque,
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::Notify,
    time::Instant,
};
mod native;
mod stream;
mod worker;
const CHUNK: usize = 4096;
const READ_SLOTS: usize = 2;
const WRITE_SLOTS: usize = 2;
#[derive(Clone)]
pub struct ControlledPipeOwner(Arc<Shared>);
pub struct ControlledPipe {
    shared: Arc<Shared>,
}
pub struct PipeClosed {
    original: std::sync::Weak<Shared>,
    acquired: bool,
}
#[derive(Debug)]
pub struct PipeNativeFailures(pub(super) Vec<Arc<io::Error>>);
impl PipeNativeFailures {
    pub fn errors(&self) -> &[Arc<io::Error>] {
        &self.0
    }
}
impl std::fmt::Display for PipeNativeFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for PipeNativeFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error.as_ref() as _)
    }
}
struct Shared {
    path: PathBuf,
    client: bool,
    tasks: WeakTaskRegistrar,
    scope_stop: crate::contract::CancellationToken,
    stop: AtomicBool,
    pending: AtomicU8,
    #[cfg(test)]
    panic_pending: AtomicBool,
    view: AtomicBool,
    wake: Mutex<Option<usize>>,
    changed: Notify,
    state: Mutex<State>,
    native: Mutex<native::Native>,
    worker: Mutex<worker::Worker>,
    witness: Mutex<Option<TaskJoinWitness>>,
    receipt: Mutex<Option<Arc<PipeClosed>>>,
}
#[derive(Default)]
struct State {
    started: bool,
    startup: Option<Result<(), Arc<io::Error>>>,
    connected: bool,
    eof: bool,
    write_closed: bool,
    read: VecDeque<Vec<u8>>,
    read_offset: usize,
    write: VecDeque<Vec<u8>>,
    writing: bool,
    fault: Option<Arc<io::Error>>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    flush_waker: Option<Waker>,
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), error.clone())
}
fn failure(error: io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(error)
}
impl ControlledPipeOwner {
    /// Complete finite payload envelope of this exact provider: the native
    /// pending buffer and two queued chunks in each direction. This excludes
    /// OS pipe buffers/allocator overhead, and grants no native/Scope authority.
    pub const fn workspace_bytes() -> usize {
        CHUNK * (READ_SLOTS + WRITE_SLOTS + 2)
    }
    /// The caller installs this cold owner and its connection/worker charge
    /// before invoking open. No OS handle/thread is created by construction.
    pub fn new(path: PathBuf, tasks: TaskRegistrar) -> Self {
        Self::with_side(path, tasks, false)
    }
    /// An explicit client of this exact named pipe. Construction is cold and
    /// retains the same original std worker, Scope and matching close duties.
    pub fn new_client(path: PathBuf, tasks: TaskRegistrar) -> Self {
        Self::with_side(path, tasks, true)
    }
    fn with_side(path: PathBuf, tasks: TaskRegistrar, client: bool) -> Self {
        let scope_stop = tasks.cancellation();
        Self(Arc::new(Shared {
            path,
            client,
            tasks: tasks.downgrade(),
            scope_stop,
            stop: AtomicBool::new(false),
            pending: AtomicU8::new(0),
            #[cfg(test)]
            panic_pending: AtomicBool::new(false),
            view: AtomicBool::new(false),
            wake: Mutex::new(None),
            changed: Notify::new(),
            state: Mutex::new(State::default()),
            native: Mutex::new(native::Native::default()),
            worker: Mutex::new(worker::Worker::default()),
            witness: Mutex::new(None),
            receipt: Mutex::new(None),
        }))
    }
    pub async fn open(&self) -> io::Result<ControlledPipe> {
        if self.0.stop.load(Ordering::Acquire) || self.0.scope_stop.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original pipe admission stopped",
            ));
        }
        {
            let mut state = lock(&self.0.state);
            if state.started {
                return Err(io::Error::other("original pipe was already started"));
            }
            state.started = true;
            self.0.view.store(true, Ordering::Release);
        }
        // This view is retained before native create or any cancellable await.
        let view = ControlledPipe {
            shared: self.0.clone(),
        };
        worker::start(&self.0)?;
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = &lock(&self.0.state).startup {
                result.as_ref().map_err(retained)?;
                return Ok(view);
            }
            notified.await;
        }
    }
    pub fn request_stop(&self) {
        self.0.request_stop();
    }
    /// Observes only actual ERROR_IO_PENDING outcomes, never authorizes IO.
    pub fn pending_io(&self) -> (bool, bool, bool) {
        let pending = self.0.pending.load(Ordering::Acquire);
        (pending & 1 != 0, pending & 2 != 0, pending & 4 != 0)
    }
    pub fn std_worker_joined(&self) -> bool {
        lock(&self.0.worker).joined.is_some()
    }
    pub fn view_returned(&self) -> bool {
        !self.0.view.load(Ordering::Acquire)
    }
    pub fn worker(&self) -> Option<TaskJoinWitness> {
        lock(&self.0.witness).clone()
    }
    pub fn closed(&self) -> Option<Arc<PipeClosed>> {
        lock(&self.0.receipt).clone()
    }
    pub fn matches_closed(&self, closed: &PipeClosed) -> bool {
        std::sync::Weak::ptr_eq(&closed.original, &Arc::downgrade(&self.0))
    }
    pub async fn close(&self, end: Instant) -> io::Result<Arc<PipeClosed>> {
        self.request_stop();
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(closed) = self.closed() {
                return Ok(closed);
            }
            // Registration failure creates no fictitious Scope join. Its same
            // retained std original is still consumed by this private owner.
            worker::join_if_finished(&self.0);
            let std_joined = lock(&self.0.worker).joined.is_some() || !lock(&self.0.state).started;
            let scope_joined = lock(&self.0.witness)
                .as_ref()
                .is_none_or(TaskJoinWitness::is_joined);
            if std_joined && scope_joined && !self.0.view.load(Ordering::Acquire) {
                let mut wake = lock(&self.0.wake);
                *wake = None;
                let (acquired, workspaces) = {
                    let mut native = lock(&self.0.native);
                    let acquired = native.acquired();
                    native.close()?;
                    (acquired, native.release_workspaces())
                };
                drop(wake);
                let queues = {
                    let mut state = lock(&self.0.state);
                    (
                        std::mem::take(&mut state.read),
                        std::mem::take(&mut state.write),
                    )
                };
                // Retained diagnostic owner clones cannot pin payload capacity
                // after positive close. No memory refund is a close receipt.
                drop((workspaces, queues));
                let closed = Arc::new(PipeClosed {
                    original: Arc::downgrade(&self.0),
                    acquired,
                });
                *lock(&self.0.receipt) = Some(closed.clone());
                return Ok(closed);
            }
            if Instant::now() >= end {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "same pipe views/std worker/Scope join remain retained",
                ));
            }
            let exited = lock(&self.0.worker).exited;
            if exited && !std_joined {
                tokio::task::yield_now().await;
                continue;
            }
            let witness = self.worker().filter(|witness| !witness.is_joined());
            tokio::time::timeout_at(end,async {
                tokio::select! {_=notified=>{},_=async {if let Some(witness)=witness {witness.wait().await;} else {std::future::pending::<()>().await}}=>{}}
            }).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"same pipe views/std worker/Scope join remain retained"))?;
        }
    }
}
impl PipeClosed {
    pub fn had_native_acquisition(&self) -> bool {
        self.acquired
    }
}
impl Shared {
    fn wake_worker(&self) {
        let error = lock(&self.wake)
            .as_ref()
            .and_then(|wake| native::signal(*wake).err());
        if let Some(error) = error {
            lock(&self.state)
                .fault
                .get_or_insert_with(|| Arc::new(error));
            self.notify_io();
        }
    }
    fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.wake_worker();
        // Stop business immediately; native drain and both real joins retain
        // their independent responsibilities and may complete later.
        self.notify_io();
    }
    fn notify_io(&self) {
        let wakes = {
            let mut state = lock(&self.state);
            [
                state.read_waker.take(),
                state.write_waker.take(),
                state.flush_waker.take(),
            ]
        };
        for wake in wakes.into_iter().flatten() {
            wake.wake();
        }
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests;
