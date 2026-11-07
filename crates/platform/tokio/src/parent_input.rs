//! One original managed-parent input, worker and Scope. EOF is not closure.
use crate::contract::{
    ChannelError, Impact, Reason, Stage,
    lifecycle::{CloseReport, Unfinished},
    scope::{TaskClass, TaskJoinWitness, TaskScope},
};
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        Arc, Condvar, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    time::Instant,
};
mod native;
#[cfg(test)]
mod tests;

/// Install this cold owner in the application before starting its original.
/// The host's stdin is borrowed only to duplicate its actual input description;
/// the duplicate is explicitly closed after its worker and Scope have joined.
pub struct ParentInputOwner {
    shared: Arc<Shared>,
    scope: AsyncMutex<TaskScope>,
}
/// Opaque receipt for this actual input duplicate and its joined worker.
/// It grants no authority over the host's borrowed original stdin.
pub struct ParentInputClosed {
    original: Weak<Shared>,
    acquired: bool,
}
impl ParentInputClosed {
    pub fn had_native_acquisition(&self) -> bool {
        self.acquired
    }
    pub fn matches(&self, owner: &ParentInputOwner) -> bool {
        self.original.ptr_eq(&Arc::downgrade(&owner.shared))
    }
}
struct Shared {
    stop: AtomicBool,
    closed: OnceLock<Arc<ParentInputClosed>>,
    changed: Notify,
    native: Mutex<native::Input>,
    worker: Mutex<Worker>,
    witness: Mutex<Option<TaskJoinWitness>>,
    result: Mutex<Option<Result<(), Arc<io::Error>>>>,
    observed: Mutex<Vec<crate::contract::scope::TaskExit>>,
}
#[derive(Default)]
struct Worker {
    started: bool,
    handle: Option<std::thread::JoinHandle<io::Result<()>>>,
    joined: bool,
    exited: bool,
    interrupt_fault: Option<Arc<io::Error>>,
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), error.clone())
}
#[derive(Debug)]
struct WorkerPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for WorkerPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = lock(&self.0);
        if let Some(message) = payload.downcast_ref::<String>() {
            write!(f, "parent input worker panicked: {message}")
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            write!(f, "parent input worker panicked: {message}")
        } else {
            f.write_str("parent input worker panicked (original payload retained)")
        }
    }
}
impl std::error::Error for WorkerPanic {}
struct Exited(Arc<Shared>);
impl Drop for Exited {
    fn drop(&mut self) {
        lock(&self.0.worker).exited = true;
        self.0.changed.notify_waiters();
    }
}
impl ParentInputOwner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(Shared {
                stop: AtomicBool::new(false),
                closed: OnceLock::new(),
                changed: Notify::new(),
                native: Mutex::new(native::Input::default()),
                worker: Mutex::new(Worker::default()),
                witness: Mutex::new(None),
                result: Mutex::new(None),
                observed: Mutex::new(Vec::new()),
            }),
            scope: AsyncMutex::new(TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            )),
        })
    }
    /// No detached worker is ever created by polling a cancellable future.
    pub fn start(&self) -> io::Result<()> {
        self.start_with(native::duplicate_stdin)
    }
    fn start_with(&self, acquire: impl FnOnce() -> io::Result<native::Input>) -> io::Result<()> {
        let scope = self
            .scope
            .try_lock()
            .map_err(|_| io::Error::other("same parent input Scope busy"))?;
        let mut worker = lock(&self.shared.worker);
        if worker.started {
            return Err(io::Error::other("same parent input already started"));
        }
        if self.shared.stop.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "parent input stopped",
            ));
        }
        let native = acquire()?;
        let input = native.raw();
        *lock(&self.shared.native) = native;
        worker.started = true;
        let latch = Arc::new((Mutex::new(false), Condvar::new()));
        let gate = latch.clone();
        let shared = self.shared.clone();
        let handle = std::thread::Builder::new()
            .name("zero-parent-input".into())
            .stack_size(128 * 1024)
            .spawn(move || {
                let _exit = Exited(shared.clone());
                let mut started = lock(&gate.0);
                while !*started {
                    started = gate
                        .1
                        .wait(started)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                drop(started);
                native::read_to_end(input, &shared.stop)
            });
        let handle = match handle {
            Ok(handle) => handle,
            Err(error) => {
                worker.joined = true;
                *lock(&self.shared.result) = Some(Err(Arc::new(error)));
                return Err(retained(
                    lock(&self.shared.result)
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .unwrap_err(),
                ));
            }
        };
        worker.handle = Some(handle);
        drop(worker);
        let shared = self.shared.clone();
        let registration = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                loop {
                    let changed = shared.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    join_if_finished(&shared);
                    if let Some(result) = &*lock(&shared.result) {
                        return result.as_ref().map(|_| ()).map_err(|error| {
                            ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                                .with_source(retained(error))
                        });
                    }
                    if lock(&shared.worker).exited {
                        tokio::task::yield_now().await;
                    } else {
                        changed.await;
                    }
                }
            });
        match registration {
            Ok((_, witness)) => *lock(&self.shared.witness) = Some(witness),
            Err(error) => {
                self.request_stop();
                *lock(&latch.0) = true;
                latch.1.notify_one();
                return Err(io::Error::other(error));
            }
        }
        *lock(&latch.0) = true;
        latch.1.notify_one();
        Ok(())
    }
    /// The same Scope drives the actual worker join before delivering EOF/error.
    pub async fn wait(&self) -> io::Result<()> {
        let mut scope = self.scope.lock().await;
        loop {
            if let Some(result) = &*lock(&self.shared.result) {
                if lock(&self.shared.witness)
                    .as_ref()
                    .is_none_or(TaskJoinWitness::is_joined)
                {
                    return result.as_ref().map(|_| ()).map_err(retained);
                }
            }
            match scope.next_exit().await {
                Some(exit) => lock(&self.shared.observed).push(exit),
                None => {
                    return Err(io::Error::other(
                        "parent input Scope ended before original worker result",
                    ));
                }
            }
        }
    }
    pub fn retirement_complete(&self) -> bool {
        self.closed().is_some_and(|closed| closed.matches(self))
    }
    pub fn closed(&self) -> Option<Arc<ParentInputClosed>> {
        self.shared.closed.get().cloned()
    }
    pub fn request_stop(&self) {
        self.shared.stop.store(true, Ordering::Release);
        let mut worker = lock(&self.shared.worker);
        if let Some(handle) = &worker.handle {
            if let Err(error) = native::interrupt(handle) {
                worker
                    .interrupt_fault
                    .get_or_insert_with(|| Arc::new(error));
            }
        }
        drop(worker);
        self.shared.changed.notify_waiters();
    }
    /// Retains the input on timeout/unknown close. Never closes host stdin.
    pub async fn shutdown(&self, deadline: Instant) -> io::Result<CloseReport> {
        self.request_stop();
        let mut report = CloseReport::default();
        let mut scope = match tokio::time::timeout_at(deadline, self.scope.lock()).await {
            Ok(scope) => scope,
            Err(_) => {
                report.unfinished.push(Unfinished::Resource(
                    "same parent input Scope consumer remains retained".into(),
                ));
                return Ok(report);
            }
        };
        loop {
            self.request_stop();
            join_if_finished(&self.shared);
            let std_joined = {
                let worker = lock(&self.shared.worker);
                !worker.started || worker.joined
            };
            if std_joined
                && lock(&self.shared.witness)
                    .as_ref()
                    .is_none_or(TaskJoinWitness::is_joined)
            {
                break;
            }
            if Instant::now() >= deadline {
                report.unfinished.push(Unfinished::Resource(
                    "original parent input worker remains retained".into(),
                ));
                return Ok(report);
            }
            // Windows synchronous cancellation can race entry into ReadFile;
            // repeat on this same retained thread until its actual join.
            tokio::select! {
                _=tokio::time::sleep_until((Instant::now()+std::time::Duration::from_millis(10)).min(deadline))=>{},
                exit=scope.next_exit()=>if let Some(exit)=exit { lock(&self.shared.observed).push(exit); },
            }
        }
        report.append(scope.shutdown(deadline).await);
        for exit in std::mem::take(&mut *lock(&self.shared.observed)) {
            record_exit(&mut report, exit);
        }
        if !report.is_complete() {
            return Ok(report);
        }
        let acquired = lock(&self.shared.worker).started;
        lock(&self.shared.native).close()?;
        self.shared.closed.get_or_init(|| {
            Arc::new(ParentInputClosed {
                original: Arc::downgrade(&self.shared),
                acquired,
            })
        });
        report
            .released_resources
            .push("same original parent input duplicate released after std and Scope joins".into());
        if let Some(error) = &lock(&self.shared.worker).interrupt_fault {
            return Err(retained(error));
        }
        if let Some(Err(error)) = &*lock(&self.shared.result) {
            return Err(retained(error));
        }
        Ok(report)
    }
}
fn record_exit(report: &mut CloseReport, exit: crate::contract::scope::TaskExit) {
    report.exited_tasks.push(exit.id);
    if let Err(error) = exit.result {
        report
            .failed_tasks
            .push(crate::contract::lifecycle::TaskFailure {
                id: exit.id,
                class: exit.class,
                error,
            });
    }
}
fn join_if_finished(shared: &Shared) {
    let mut worker = lock(&shared.worker);
    if worker.joined
        || worker
            .handle
            .as_ref()
            .is_none_or(|handle| !handle.is_finished())
    {
        return;
    }
    let handle = worker.handle.take().expect("retained actual parent worker");
    let result = match handle.join() {
        Ok(result) => result.map_err(Arc::new),
        Err(payload) => Err(Arc::new(io::Error::other(WorkerPanic(Mutex::new(payload))))),
    };
    worker.joined = true;
    *lock(&shared.result) = Some(result);
    drop(worker);
    shared.changed.notify_waiters();
}
