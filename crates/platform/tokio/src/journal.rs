//! One declared regular-file journal, without exporting a file or raw handle.
//! A custody reservation covers the whole lock/read/append/replace graph; it
//! counts neither descriptors nor resident memory. No task or worker is spawned.
use crate::{
    contract::{scope::ScopeRetirementObservation, CancellationToken, ChannelError},
    controlled_carrier::CarrierOriginalTotals,
    file::{OriginalFile, OriginalFileClosed},
};
use std::{
    io,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};
use zero_core::component::ComponentInstanceId;
mod io_ops;
mod lifecycle;
mod retirement;
mod rotation;
mod slots;
use slots::State;

/// The exact path and restart-readable payload envelope. Temporary and lock
/// names are derived internally; this grants no arbitrary file-tree access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSpec {
    pub path: PathBuf,
    pub max_bytes: NonZeroUsize,
}
#[derive(Clone)]
pub struct JournalGrant(Arc<Core>);
/// Cold durable controller. Keep it before opening; grants confer no close.
pub struct JournalOwner(Arc<Core>);
struct Core {
    spec: JournalSpec,
    hosted: Option<Hosted>,
    state: Mutex<State>,
    native_fault: Mutex<Option<Arc<ChannelError>>>,
    closing: AtomicBool,
    changed: Arc<tokio::sync::Notify>,
}
struct Hosted {
    instance: ComponentInstanceId,
    root: Weak<CarrierOriginalTotals>,
    scope: ScopeRetirementObservation,
    stop: CancellationToken,
    reserved: Mutex<bool>,
    waiting: retirement::Waiting,
}
#[derive(Clone, Debug)]
pub struct JournalClosed(Arc<CoreIdentity>);
#[derive(Debug)]
struct CoreIdentity;
/// Work and physical-close failures have separate retained typed sources.
#[derive(Debug, Clone)]
pub struct JournalFailure {
    work: Option<Arc<io::Error>>,
    cleanup: Option<Arc<io::Error>>,
}
impl JournalFailure {
    pub fn work(&self) -> Option<&Arc<io::Error>> {
        self.work.as_ref()
    }
    pub fn cleanup(&self) -> Option<&Arc<io::Error>> {
        self.cleanup.as_ref()
    }
}
impl std::fmt::Display for JournalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(work) = &self.work {
            work.fmt(f)?;
        }
        if let Some(cleanup) = &self.cleanup {
            write!(f, "; journal close: {cleanup}")?;
        }
        Ok(())
    }
}
impl std::error::Error for JournalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.work
            .as_deref()
            .or(self.cleanup.as_deref())
            .map(|error| error as _)
    }
}
impl JournalOwner {
    /// Identity only; no filesystem work, runtime entry, task or reservation.
    pub fn new_hosted(
        spec: JournalSpec,
        instance: ComponentInstanceId,
        scope: ScopeRetirementObservation,
        stop: CancellationToken,
        root: &Arc<CarrierOriginalTotals>,
    ) -> io::Result<Self> {
        Self::construct(
            spec,
            Some(Hosted {
                instance,
                scope,
                stop,
                root: Arc::downgrade(root),
                reserved: Mutex::new(false),
                waiting: retirement::Waiting::default(),
            }),
        )
    }
    /// Explicit genuine library ownership. Production uses its hosted grant.
    pub fn standalone(spec: JournalSpec) -> io::Result<Self> {
        Self::construct(spec, None)
    }
    fn construct(spec: JournalSpec, hosted: Option<Hosted>) -> io::Result<Self> {
        if spec.path.file_name().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal needs a file path",
            ));
        }
        Ok(Self(Arc::new(Core {
            spec,
            hosted,
            state: Mutex::new(State::default()),
            native_fault: Mutex::default(),
            closing: AtomicBool::new(false),
            changed: Arc::default(),
        })))
    }
    pub fn grant(&self) -> JournalGrant {
        JournalGrant(self.0.clone())
    }
    /// Charge and strongly retain this exact graph before the first real open.
    pub fn start(&self) -> io::Result<()> {
        self.0.start()
    }
    /// Only observes/advances this journal's real calls and originals. It never
    /// waits for its own caller/Factory Scope to join or manufactures an ACK.
    pub fn try_close(&self) -> io::Result<Option<JournalClosed>> {
        self.0.try_close()
    }
    pub fn closed(&self) -> Option<JournalClosed> {
        match self.0.state.try_lock() {
            Ok(state) => state.closed.clone(),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().closed.clone(),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
    pub fn matches_closed(&self, receipt: &JournalClosed) -> bool {
        let state = match self.0.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };
        Arc::ptr_eq(&state.identity, &receipt.0) && state.closed.is_some()
    }
    pub fn max_bytes(&self) -> usize {
        self.0.spec.max_bytes.get()
    }
}
impl JournalGrant {
    pub fn path(&self) -> &std::path::Path {
        &self.0.spec.path
    }
    pub fn max_bytes(&self) -> usize {
        self.0.spec.max_bytes.get()
    }
    /// Positive same-controller native state only; never the Scope ACK.
    pub fn native_closed(&self) -> bool {
        match self.0.state.try_lock() {
            Ok(state) => state.closed.is_some(),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().closed.is_some(),
            Err(std::sync::TryLockError::WouldBlock) => false,
        }
    }
    pub fn host_scope(
        &self,
    ) -> Option<(
        ComponentInstanceId,
        crate::contract::scope::TaskScopeIdentity,
    )> {
        self.0
            .hosted
            .as_ref()
            .map(|hosted| (hosted.instance, hosted.scope.scope_identity()))
    }
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Core {
    fn require_scope(&self) -> io::Result<()> {
        if self
            .hosted
            .as_ref()
            .is_some_and(|hosted| !hosted.scope.scope_identity().resource_open())
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "journal Scope admission is closed",
            ));
        }
        Ok(())
    }
    fn call_state(&self) -> io::Result<slots::Operation<'_>> {
        match self.state.try_lock() {
            Ok(state) => Ok(slots::Operation::new(state, &self.changed)),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                Ok(slots::Operation::new(error.into_inner(), &self.changed))
            }
            Err(std::sync::TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "journal already has its one actual IO call",
            )),
        }
    }
    #[cfg(test)]
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn require_open(&self, state: &State) -> io::Result<()> {
        if self.closing.load(Ordering::Acquire)
            || state.closing
            || !state.started
            || self
                .hosted
                .as_ref()
                .is_some_and(|hosted| hosted.stop.is_cancelled())
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "journal admission is closed",
            ));
        }
        Ok(())
    }
    fn reserve(self: &Arc<Self>) -> io::Result<()> {
        if let Some(hosted) = &self.hosted {
            let mut reserved = hosted
                .reserved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !*reserved {
                if hosted.stop.is_cancelled() || !hosted.scope.scope_identity().resource_open() {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "journal Scope is not live",
                    ));
                }
                let root = hosted.root.upgrade().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "journal Host retired")
                })?;
                root.reserve_source(hosted.instance, self.clone())
                    .map_err(|source| {
                        let kind = if source.reason == crate::contract::Reason::Capacity {
                            io::ErrorKind::WouldBlock
                        } else {
                            io::ErrorKind::Other
                        };
                        io::Error::new(kind, source)
                    })?;
                *reserved = true;
            }
        }
        Ok(())
    }
}
#[cfg(test)]
#[path = "../tests/journal/mod.rs"]
mod tests;
