//! Only original entries own connection/worker charges, including failed opens.
use super::*;
use std::sync::Mutex;
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
pub(super) struct Entry {
    pub(super) original: ControlledPipeOwner,
    charge: Mutex<Option<ControlBudgetPermit>>,
    pub(super) task: Mutex<Option<tokio::task::Id>>,
    pub(super) joined: AtomicBool,
}
#[derive(Default)]
pub(super) struct Originals(Mutex<Vec<Arc<Entry>>>);
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
impl Originals {
    pub(super) fn is_empty(&self) -> bool {
        lock(&self.0).is_empty()
    }
    pub(super) fn prepare(
        &self,
        path: &std::path::Path,
        tasks: zero_platform_tokio::contract::scope::TaskRegistrar,
        budget: &Arc<zero_runtime::configuration::ControlBudget>,
    ) -> io::Result<Arc<Entry>> {
        // First charge, then install this concrete cold owner, then create its
        // native pipe/worker. A capacity miss issues no CreateNamedPipe/thread.
        let charge = budget.reserve(ControlCharge::Connection)?;
        let entry = Arc::new(Entry {
            original: ControlledPipeOwner::new(path.to_owned(), tasks),
            charge: Mutex::new(Some(charge)),
            task: Mutex::new(None),
            joined: AtomicBool::new(false),
        });
        lock(&self.0).push(entry.clone());
        Ok(entry)
    }
    pub(super) fn failed_open(&self, entry: &Entry) {
        entry.joined.store(true, Ordering::Release);
        entry.original.request_stop();
    }
    pub(super) fn observe_joined(&self, id: tokio::task::Id) {
        for entry in lock(&self.0).iter() {
            if *lock(&entry.task) == Some(id) {
                entry.joined.store(true, Ordering::Release);
            }
        }
    }
    pub(super) fn request_stop(&self) {
        for entry in lock(&self.0).iter() {
            entry.original.request_stop();
        }
    }
    pub(super) async fn reap(&self, all: bool) -> io::Result<()> {
        let originals = lock(&self.0).clone();
        let mut failures = Vec::new();
        for entry in originals {
            if !all
                && (!entry.joined.load(Ordering::Acquire)
                    || !entry.original.view_returned()
                    || entry
                        .original
                        .worker()
                        .is_some_and(|worker| !worker.is_joined()))
            {
                continue;
            }
            match entry
                .original
                .close(super::super::owner::close_deadline())
                .await
            {
                Ok(closed) if entry.original.matches_closed(&closed) => {
                    lock(&entry.charge).take();
                    lock(&self.0).retain(|actual| !Arc::ptr_eq(actual, &entry));
                }
                Ok(_) => failures.push(io::Error::other("foreign actual pipe close receipt")),
                Err(error) => failures.push(error),
            }
        }
        finish(failures)
    }
}
#[derive(Debug)]
struct Failures(Vec<io::Error>);
impl std::fmt::Display for Failures {
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
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error as _)
    }
}
pub(super) fn finish(mut errors: Vec<io::Error>) -> io::Result<()> {
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        _ => Err(io::Error::other(Failures(errors))),
    }
}

#[derive(Debug)]
pub(super) struct ScopeFault(pub(super) zero_platform_tokio::contract::lifecycle::CloseReport);
impl std::fmt::Display for ScopeFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "actual pipe Scope cleanup: {:?}", self.0)
    }
}
impl std::error::Error for ScopeFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.failed_tasks.first().map(|failed| &failed.error as _)
    }
}
