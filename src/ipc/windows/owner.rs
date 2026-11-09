//! Only original entries own connection/worker charges, including failed opens.
use super::*;
use std::sync::Mutex;
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
pub(super) struct Entry {
    ordinal: u64,
    pub(super) original: ControlledPipeOwner,
    charge: Mutex<Option<(ControlBudgetPermit, ControlBudgetPermit)>>,
    pub(super) task: Mutex<Option<tokio::task::Id>>,
    pub(super) joined: AtomicBool,
}
impl Entry {
    /// The same first-open Connection charge issues one local book. No new
    /// connection, worker or workspace allocation is performed here.
    pub(super) fn connection_budget(
        &self,
    ) -> Option<zero_runtime::configuration::ControlConnectionBudget> {
        lock(&self.charge).as_ref()?.0.connection_budget()
    }
}
#[derive(Default)]
pub(super) struct Originals(Mutex<Vec<Arc<Entry>>>, AtomicU64);
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
impl Originals {
    #[cfg(test)]
    pub(super) fn originals_for_test(&self) -> Vec<Arc<Entry>> {
        lock(&self.0).clone()
    }
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
        let workspace = budget.reserve(ControlCharge::PipeWorkspaceBytes(
            ControlledPipeOwner::workspace_bytes(),
        ))?;
        let ordinal = self
            .1
            .try_update(Ordering::AcqRel, Ordering::Acquire, |previous| {
                previous.checked_add(1)
            })
            .map_err(|_| io::Error::other("original pipe sequence exhausted"))?
            + 1;
        let entry = Arc::new(Entry {
            ordinal,
            original: ControlledPipeOwner::new(path.to_owned(), tasks),
            charge: Mutex::new(Some((charge, workspace))),
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
        // An original may synchronously wake an external IO waiter. Retain
        // only this one actual entry before leaving the registration lock.
        let last = self.1.load(Ordering::Acquire);
        let mut cursor = 0;
        loop {
            let next = lock(&self.0)
                .iter()
                .filter(|entry| entry.ordinal > cursor && entry.ordinal <= last)
                .min_by_key(|entry| entry.ordinal)
                .cloned();
            let Some(entry) = next else { break };
            cursor = entry.ordinal;
            entry.original.request_stop();
        }
    }
    pub(super) async fn reap(&self, all: bool) -> io::Result<()> {
        // Borrow one already charged original at a time. Do not allocate an
        // uncharged snapshot proportional to every pending/failed connection.
        // Concurrent cleanup may remove entries; immutable ordinals prevent
        // that from skipping another original or admitting a replacement ABA.
        let last = self.1.load(Ordering::Acquire);
        let mut cursor = 0;
        let mut failures = Vec::new();
        loop {
            let next = lock(&self.0)
                .iter()
                .filter(|entry| entry.ordinal > cursor && entry.ordinal <= last)
                .min_by_key(|entry| entry.ordinal)
                .cloned();
            let Some(entry) = next else { break };
            cursor = entry.ordinal;
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
                    let peer_joined =
                        lock(&entry.task).is_none() || entry.joined.load(Ordering::Acquire);
                    let acknowledged = entry.original.worker().is_none_or(|worker| {
                        worker.try_joined().is_some_and(|joined| {
                            worker.matches_joined(&joined) && joined.registration_released()
                        })
                    });
                    if !peer_joined || !acknowledged {
                        if all {
                            failures.push(io::Error::new(io::ErrorKind::WouldBlock,
                            "same pipe connection/std worker Scope acknowledgement remains retained"));
                        }
                        continue;
                    }
                    let charges = lock(&entry.charge).take();
                    drop(charges);
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

#[cfg(test)]
#[path = "owner/stop.rs"]
mod stop;
