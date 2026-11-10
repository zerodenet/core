//! Explicit genuine Engine-only worker; its original thread migrates by join.
use super::*;
use std::{panic::{catch_unwind, resume_unwind, AssertUnwindSafe}, sync::Condvar, thread::JoinHandle};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};

#[derive(Default)]
pub(super) struct Signal {
    flags: Mutex<Flags>,
    changed: Condvar,
}
#[derive(Default)]
struct Flags { dirty: bool, stopping: bool }
pub(super) struct Actor {
    pub(super) original: Option<JoinHandle<io::Result<()>>>,
    pub(super) id: std::thread::ThreadId,
    pub(super) joined: Option<Result<(), Arc<io::Error>>>,
    pub(super) signal: Arc<Signal>,
}
impl Signal {
    pub(super) fn dirty(&self) {
        self.flags.lock().unwrap_or_else(|error| error.into_inner()).dirty = true;
        self.changed.notify_all();
    }
    pub(super) fn shutdown(&self) {
        let mut flags = self.flags.lock().unwrap_or_else(|error| error.into_inner());
        flags.stopping = true;
        flags.dirty = true;
        drop(flags);
        self.changed.notify_all();
    }
}
impl Actor {
    pub(super) fn start(storage: WeakQuotaStorage, producer: Producer, gate: Arc<Gate>) -> io::Result<Self> {
        let signal = Arc::new(Signal::default());
        let worker_signal = signal.clone();
        let original = std::thread::Builder::new().name("zero-quota-state".to_owned()).spawn(move || {
            gate.wait();
            let result = catch_unwind(AssertUnwindSafe(|| run(&storage, &producer, &worker_signal)));
            // This same real worker closes after its last write. A migration
            // never registers a watcher or calls std.join inside a Scope job.
            let cleanup = storage.upgrade().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "quota original owner retired"))
                .and_then(|storage| storage.close_lease());
            match result {
                Ok(work) => finish(work, cleanup),
                Err(original) => {
                    // close_lease already retains any independent original OS
                    // failure. Resume this exact panic Box for std.join Err.
                    drop(cleanup);
                    resume_unwind(original)
                }
            }
        })?;
        let id = original.thread().id();
        Ok(Self { original: Some(original), id, joined: None, signal })
    }
    pub(super) fn is_finished(&self) -> bool {
        self.original.as_ref().is_none_or(JoinHandle::is_finished)
    }
    pub(super) fn collect(&mut self, failure: &mut FailureSlot, cleanup: Option<Arc<io::Error>>) -> io::Result<()> {
        let Some(original) = self.original.take() else { return Ok(()); };
        if !original.is_finished() { self.original = Some(original); return Err(io::ErrorKind::WouldBlock.into()); }
        self.joined = Some(match original.join() {
            Ok(result) => result.map_err(Arc::new),
            Err(payload) => Err(failure.panic(self.id, payload, cleanup)),
        });
        Ok(())
    }
    /// All transient ordinary mark_dirty callers have released this exact
    /// signal allocation before Kernel headers can reuse its memory promise.
    pub(super) fn collect_signal(&mut self) -> bool { Arc::get_mut(&mut self.signal).is_some() }
}
fn run(storage: &WeakQuotaStorage, producer: &Producer, signal: &Signal) -> io::Result<()> {
    loop {
        let stopping = {
            let mut flags = signal.flags.lock().unwrap_or_else(|error| error.into_inner());
            while !flags.dirty { flags = signal.changed.wait(flags).unwrap_or_else(|error| error.into_inner()); }
            if !flags.stopping {
                let waited = signal.changed.wait_timeout(flags, std::time::Duration::from_millis(50)).unwrap_or_else(|error| error.into_inner());
                flags = waited.0;
            }
            flags.dirty = false;
            flags.stopping
        };
        let storage = storage.upgrade().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "quota original owner retired"))?;
        let Some(cell) = storage.execution.take_write_cell(stopping)? else {
            drop(storage);
            // Existing finite worker's bounded pressure wait; no new task,
            // native open, busy loop or discarded accepted dirty fact.
            signal.dirty();
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        };
        let result = producer.write();
        let error = storage.execution.receive_write(result, cell, stopping);
        storage.execution.changed.notify_waiters();
        if stopping { return error.map_or(Ok(()), |original| Err(retained(&original))); }
    }
}
fn finish<T>(work: io::Result<T>, cleanup: io::Result<()>) -> io::Result<T> {
    match (work, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(work), Err(cleanup)) => Err(io::Error::other(AcquisitionCleanupFailure { acquisition: work, cleanup })),
    }
}

pub(super) struct FailureSlot(Option<QuotaMemoryCredit>);
struct Panic {
    thread: std::thread::ThreadId,
    original: Mutex<Box<dyn std::any::Any + Send>>,
    cleanup: Option<Arc<io::Error>>,
}
#[derive(Clone)]
struct PanicView(PacketMetadataArc<Panic>);
impl FailureSlot {
    pub(super) fn reserve(memory: &QuotaMemoryBudget) -> io::Result<Self> {
        memory.reserve(PacketMetadataArc::<Panic>::layout().map_err(io::Error::other)?).map(|credit| Self(Some(credit)))
    }
    fn panic(&mut self, thread: std::thread::ThreadId, original: Box<dyn std::any::Any + Send>, cleanup: Option<Arc<io::Error>>) -> Arc<io::Error> {
        let value = Panic { thread, original: Mutex::new(original), cleanup };
        let value = match PacketMetadataArc::from_prepaid(value, self.0.take().expect("one actual std worker panic").into_metadata()) {
            Ok(value) => value, Err(_) => unreachable!("same prepaid actual std failure layout"),
        };
        Arc::new(io::Error::other(PanicView(value)))
    }
}
impl std::fmt::Debug for PanicView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("QuotaOriginalStdPanic").field("thread", &self.0.thread).finish_non_exhaustive() }
}
impl std::fmt::Display for PanicView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let original = self.0.original.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(message) = original.downcast_ref::<String>() { write!(f, "quota worker panicked: {message}")?; }
        else if let Some(message) = original.downcast_ref::<&str>() { write!(f, "quota worker panicked: {message}")?; }
        else { f.write_str("quota worker panicked (original payload retained)")?; }
        if let Some(cleanup) = &self.0.cleanup { write!(f, "; quota lease cleanup failed: {cleanup}")?; }
        Ok(())
    }
}
impl std::error::Error for PanicView {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { self.0.cleanup.as_deref().map(|error| error as _) }
}
