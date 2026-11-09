//! Concrete process logging. Independent output actors share one finite book.
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    task::Poll,
};
use zero_config::LogConfig;
use zero_platform_tokio::contract::scope::{
    ScopeRetirementObservation, TaskClass, TaskJoinWitness, TaskRegistrar, TaskScopeIdentity,
};
mod archive;
mod book;
mod drive;
mod fault;
mod file;
mod queue;
mod wake;
mod worker;
pub use archive::LoggingFailure;
pub use book::{LoggingBudgetCandidate, LoggingHandle, Usage as LoggingUsage};
pub use fault::{IoFailure, IoPhase, WorkerFailure};
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(capacity)
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "actual logging owner capacity exhausted",
    )
}
struct Shared {
    entries: Vec<file::Output>,
    book: Arc<book::Book>,
    scope: TaskScopeIdentity,
    _entries_storage: book::Storage,
}
struct Lane {
    witness: Option<TaskJoinWitness>,
    creating: bool,
    slot: Option<fault::WorkerSlot>,
    delivery: Option<WorkerFailure>,
    failure_delivered: bool,
}
pub struct ScopedTracing {
    shared: Arc<Shared>,
    scope: ScopeRetirementObservation,
    lanes: Vec<Lane>,
    archive: archive::Archive,
    // Actual lane array is destroyed before its capacity credit.
    _lanes_storage: book::Storage,
    stopping: bool,
}
impl ScopedTracing {
    /// main owns this cold controller and the actual Scope before register,
    /// installation of the tracing layers, or the first configured file open.
    pub fn prepare(config: &LogConfig, tasks: &TaskRegistrar) -> io::Result<Self> {
        let limits = book::Limits::new(config)?;
        let book = book::Book::new(limits, config.files.len())?;
        let layout = zero_traits::packet_metadata::PacketMetadataLayout::array::<file::Output>(
            config.files.len(),
        )
        .map_err(io::Error::other)?;
        let entries_storage = book.layout(layout)?;
        let lane_layout =
            zero_traits::packet_metadata::PacketMetadataLayout::array::<Lane>(config.files.len())
                .map_err(io::Error::other)?;
        let lanes_storage = book.layout(lane_layout)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(config.files.len())
            .map_err(io::Error::other)?;
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(config.files.len())
            .map_err(io::Error::other)?;
        for (index, file) in config.files.iter().enumerate() {
            let slot = fault::WorkerSlot::reserve(&book)?;
            entries.push(file::Entry::prepare(index, file, &book)?);
            lanes.push(Lane {
                witness: None,
                creating: false,
                slot: Some(slot),
                delivery: None,
                failure_delivered: false,
            });
        }
        Ok(Self {
            shared: Arc::new(Shared {
                entries,
                book,
                scope: tasks.scope_identity(),
                _entries_storage: entries_storage,
            }),
            scope: tasks.retirement_observation(),
            lanes,
            archive: archive::Archive::new(),
            _lanes_storage: lanes_storage,
            stopping: false,
        })
    }
    pub fn budget_handle(&self) -> LoggingHandle {
        LoggingHandle::new(self.shared.book.clone())
    }
    pub fn usage(&self) -> LoggingUsage {
        self.shared.book.usage()
    }
    pub fn writer(&self, output: usize) -> io::Result<OwnedWriter> {
        let entry = self.shared.entries.get(output).ok_or_else(|| {
            io::Error::new(io::ErrorKind::PermissionDenied, "foreign logging output")
        })?;
        Ok(OwnedWriter {
            entry: entry.clone(),
        })
    }
    /// Pure wake registration; main checks actual predicates again after arm.
    pub fn arm_changes(&self) {
        self.shared.book.wake.arm();
    }
    pub async fn changed(&self) {
        std::future::poll_fn(|cx| {
            self.shared.book.wake.register(cx.waker());
            if self.shared.book.wake.changed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
    pub fn native_closed(&self) -> bool {
        self.shared
            .entries
            .iter()
            .all(|entry| entry.native_closed())
    }
    pub fn ready(&self) -> bool {
        self.shared.entries.iter().all(|entry| entry.initialized())
    }
    pub fn initialization_failed(&self) -> bool {
        self.shared
            .entries
            .iter()
            .any(|entry| entry.failed_initialization())
    }
    pub fn request_stop(&mut self) {
        self.stopping = true;
        self.shared.book.seal();
        for entry in &self.shared.entries {
            entry.stop();
        }
    }
    pub fn peek_io_failure(&self) -> Option<IoFailure> {
        self.shared
            .entries
            .iter()
            .find_map(|entry| entry.peek_failure())
    }
    pub fn accept_io_failure(&self, source: &IoFailure) -> bool {
        self.shared
            .entries
            .get(source.output())
            .is_some_and(|entry| entry.accept_failure(source))
    }
    /// No possibly-blocked native lock is acquired before the exact original
    /// actor witnesses join and release their registrations.
    pub fn close_after_workers(&mut self) -> bool {
        if !self.scope.is_drained() {
            return false;
        }
        if self.lanes.iter().any(|lane| {
            lane.creating
                || lane.witness.as_ref().is_some_and(|w| {
                    w.try_joined()
                        .is_none_or(|joined| !joined.registration_released())
                })
        }) {
            return false;
        }
        let complete = self
            .shared
            .entries
            .iter()
            .all(|entry| entry.native_closed());
        if complete {
            // Destroy actual successful local observers before returning their
            // header promise. Failure guards remain in the paid archive.
            for lane in &mut self.lanes {
                drop(lane.witness.take());
                drop(lane.slot.take());
            }
            self.shared.book.release_outputs();
        }
        self.shared.book.wake.clear();
        complete
    }
    #[cfg(test)]
    pub(crate) fn witness(&self, output: usize) -> Option<TaskJoinWitness> {
        self.lanes.get(output)?.witness.clone()
    }
}
impl Drop for ScopedTracing {
    fn drop(&mut self) {
        self.request_stop();
        self.shared.book.wake.clear();
    }
}
#[derive(Clone)]
pub struct OwnedWriter {
    entry: file::Output,
}
impl Write for OwnedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if !self.entry.stopping() {
            if let Ok(frame) = queue::Frame::prepare(&self.entry.book, self.entry.output, bytes) {
                let _ = self.entry.enqueue(Some(frame));
            }
        }
        // Original lossy writer semantics: a full book rejects the incoming
        // chunk, never an earlier admitted chunk or application continuation.
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for OwnedWriter {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
#[path = "../tests/owned/mod.rs"]
mod tests;
