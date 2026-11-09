//! One real application attempt survives caller cancellation; no daemon Host.
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, OnceLock,
};
use tokio::sync::{oneshot, Notify};
use zero_platform_tokio::contract::{
    scope::{HostedScope, TaskJoined},
    CancellationToken, ChannelError,
};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
mod handoff;
mod retire;
mod work;
static ACTIVE: Mutex<Option<Arc<Attempt>>> = Mutex::new(None);
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
pub(crate) struct Caller {
    pub(super) original: Arc<Attempt>,
}
pub(super) struct Attempt {
    pub(super) book: Arc<ControlBudget>,
    stop: CancellationToken,
    gone: AtomicBool,
    started: AtomicBool,
    retired: AtomicBool,
    fault_delivered: AtomicBool,
    end: OnceLock<tokio::time::Instant>,
    driver: crate::ipc::owner::ControlTask,
    scope: tokio::sync::Mutex<Option<HostedScope>>,
    native: tokio::sync::Mutex<native::Native>,
    charge: Mutex<Option<(ControlBudgetPermit, Option<ControlBudgetPermit>)>>,
    faults: Faults,
    changed: Notify,
    #[cfg(test)]
    read_pending: AtomicBool,
    #[cfg(test)]
    panic_pending: AtomicBool,
    #[cfg(test)]
    fatal_decode: AtomicBool,
}
#[derive(Default)]
pub(super) struct Faults {
    joined: Mutex<Vec<TaskJoined>>,
    native: Mutex<Option<Arc<ChannelError>>>,
    abandoned: Mutex<Option<Arc<io::Error>>>,
}
impl Faults {
    pub(super) fn native(&self, error: ChannelError) -> Arc<ChannelError> {
        // The provider canonical owns its late pure-source ledger. Re-observing
        // it must not append another wrapper on each retirement retry.
        lock(&self.native)
            .get_or_insert_with(|| Arc::new(error))
            .clone()
    }
    fn report(&self) -> io::Result<()> {
        let joined = lock(&self.joined).clone();
        let native = lock(&self.native).clone();

        if native.is_none() && joined.iter().all(|row| row.result().is_ok()) {
            return Ok(());
        }
        let errors = joined.iter().filter_map(|row| row.result().err()).collect();
        Err(io::Error::other(ClientFaults {
            joined,
            errors,
            native,
        }))
    }
}
struct ClientFaults {
    joined: Vec<TaskJoined>,
    errors: Vec<Arc<ChannelError>>,
    native: Option<Arc<ChannelError>>,
}
impl std::fmt::Debug for ClientFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientFaults")
            .field("tasks", &self.joined.len())
            .field("native", &self.native.is_some())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for ClientFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("control client original cleanup failed")
    }
}
impl std::error::Error for ClientFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(source) = &self.native {
            return Some(source.as_ref());
        }
        self.errors.first().map(|error| error.as_ref() as _)
    }
}
impl Caller {
    pub(crate) async fn begin(limits: Limits) -> io::Result<Self> {
        let limits = limits.validate()?;
        let old = lock(&ACTIVE).clone();
        let book = if let Some(old) = old.as_ref() {
            if !old.gone.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "control client already has a live caller",
                ));
            }
            old.stop.cancel();
            let end = crate::ipc::owner::close_deadline();
            let joined = tokio::time::timeout_at(end, old.driver.join())
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "same control client driver remains retained",
                    )
                })?;
            if !old.retired.load(Ordering::Acquire) {
                old.progress(end).await?;
            }
            if !old.retired.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "same client originals remain unclosed",
                ));
            }
            // The actual raw driver is joined, the Scope deliveries accepted,
            // and native/view disposition is positive. Payload credits are
            // independent and remain on this same application book.
            drop(lock(&old.charge).take());
            old.handoff(joined)?;
            let book = old.book.clone();
            let target = limits.budget_limits();
            if book.limits() != target {
                book.prepare(target)?.commit()?;
            }
            book
        } else {
            ControlBudget::new(limits.budget_limits())
        };
        let charge = book.reserve(ControlCharge::Connection)?;
        #[cfg(windows)]
        let workspace = Some(book.reserve(ControlCharge::PipeWorkspaceBytes(
            zero_platform_tokio::ControlledPipeOwner::workspace_bytes(),
        ))?);
        #[cfg(not(windows))]
        let workspace = None;
        let original = Arc::new(Attempt {
            book,
            stop: CancellationToken::new(),
            gone: AtomicBool::new(false),
            started: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            fault_delivered: AtomicBool::new(false),
            end: OnceLock::new(),
            driver: crate::ipc::owner::ControlTask::empty(),
            scope: Default::default(),
            native: tokio::sync::Mutex::new(native::Native::Cold),
            charge: Mutex::new(Some((charge, workspace))),
            faults: Faults::default(),
            changed: Notify::new(),
            #[cfg(test)]
            read_pending: AtomicBool::new(false),
            #[cfg(test)]
            panic_pending: AtomicBool::new(false),
            #[cfg(test)]
            fatal_decode: AtomicBool::new(false),
        });
        let mut active = lock(&ACTIVE);
        let unchanged = match (&*active, &old) {
            (None, None) => true,
            (Some(current), Some(old)) => Arc::ptr_eq(current, old),
            _ => false,
        };
        if !unchanged {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "control client attempt raced another caller",
            ));
        }
        *active = Some(original.clone());
        Ok(Self { original })
    }
    pub(crate) async fn finish(self) -> io::Result<()> {
        self.original.stop.cancel();
        let end = *self
            .original
            .end
            .get_or_init(crate::ipc::owner::close_deadline);
        let joined = tokio::time::timeout_at(end, self.original.driver.join())
            .await
            .map_err(|_| {
                self.original
                    .incomplete("same client task remains retained")
            })?;
        if !self.original.retired.load(Ordering::Acquire) {
            self.original.progress(end).await?;
        }
        if !self.original.retired.load(Ordering::Acquire) {
            return Err(self
                .original
                .incomplete("same client originals remain retained"));
        }
        drop(lock(&self.original.charge).take());
        self.original.handoff(joined)
    }
}
impl Drop for Caller {
    fn drop(&mut self) {
        self.original.gone.store(true, Ordering::Release);
        self.original.stop.cancel();
        self.original.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests;

impl Attempt {
    fn incomplete(&self, message: &'static str) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            ClientIncomplete {
                message,
                fault: self.faults.report().err(),
            },
        )
    }
}
#[derive(Debug)]
struct ClientIncomplete {
    message: &'static str,
    fault: Option<io::Error>,
}
impl std::fmt::Display for ClientIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}
impl std::error::Error for ClientIncomplete {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.fault.as_ref().map(|error| error as _)
    }
}
