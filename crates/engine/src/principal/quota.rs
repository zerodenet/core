//! Principal traffic quota accounting and durable recovery state.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{error, info};
use zero_core::SessionAuth;

use crate::EngineError;
use zero_runtime::configuration::{ConfigurationSource, QuotaStorage};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct QuotaKey {
    principal_key: String,
    policy_revision: Option<u64>,
    initial_bytes: u64,
}

#[derive(Debug)]
struct QuotaState {
    remaining_bytes: u64,
    references: usize,
}

#[derive(Debug, Default)]
struct QuotaBook {
    current: HashMap<String, QuotaKey>,
    states: HashMap<QuotaKey, QuotaState>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedQuotaBook {
    version: u32,
    balances: Vec<PersistedQuotaBalance>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedQuotaBalance {
    key: QuotaKey,
    remaining_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrincipalQuotaStateReport {
    pub format: &'static str,
    pub path: String,
    pub status: PrincipalQuotaStateStatus,
    pub bytes: u64,
    pub balances: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalQuotaStateStatus {
    Missing,
    Ready,
    Incompatible,
}

impl PrincipalQuotaStateReport {
    pub fn is_compatible(&self) -> bool {
        self.status != PrincipalQuotaStateStatus::Incompatible
    }
}

#[derive(Debug)]
enum PersistenceMessage {
    Dirty,
    Shutdown,
}

struct QuotaPersistence {
    storage: Arc<QuotaStorage>,
    source: Arc<ConfigurationSource>,
    sender: SyncSender<PersistenceMessage>,
    worker: Mutex<Worker>,
    last_error: Arc<Mutex<Option<Arc<io::Error>>>>,
}
struct Worker {
    original: Option<JoinHandle<io::Result<()>>>,
    joined: Option<Result<(), Arc<io::Error>>>,
}
impl std::fmt::Debug for QuotaPersistence {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("QuotaPersistence")
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}
struct WorkerPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for WorkerPanic {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("QuotaWorkerPanic").finish_non_exhaustive()
    }
}
impl std::fmt::Display for WorkerPanic {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(message) = payload.downcast_ref::<String>() {
            write!(fmt, "quota worker panicked: {message}")
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            write!(fmt, "quota worker panicked: {message}")
        } else {
            fmt.write_str("quota worker panicked (original payload retained)")
        }
    }
}
impl std::error::Error for WorkerPanic {}
#[derive(Debug)]
struct CleanupFailure {
    work: io::Error,
    cleanup: io::Error,
}
impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            fmt,
            "{}; quota lease cleanup failed: {}",
            self.work, self.cleanup
        )
    }
}
impl std::error::Error for CleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.work)
    }
}
fn finish_cleanup<T>(work: io::Result<T>, cleanup: io::Result<()>) -> io::Result<T> {
    match (work, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(work), Err(cleanup)) => Err(io::Error::other(CleanupFailure { work, cleanup })),
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), error.clone())
}

#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistry {
    inner: Arc<Mutex<QuotaBook>>,
    persistence: Option<Arc<QuotaPersistence>>,
    stopping: AtomicBool,
}

impl Default for PrincipalQuotaRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QuotaBook::default())),
            persistence: None,
            stopping: AtomicBool::new(false),
        }
    }
}

impl PrincipalQuotaRegistry {
    pub(crate) fn open(
        state_path: Option<PathBuf>,
        source: Arc<ConfigurationSource>,
    ) -> io::Result<Self> {
        let Some(state_path) = state_path else {
            return Ok(Self::default());
        };
        let storage = source.open_quota_storage(state_path)?;
        let book = match read_book(&storage) {
            Ok(book) => book,
            Err(error) => return finish_cleanup(Err(error), storage.close_lease()),
        };
        let inner = Arc::new(Mutex::new(book));
        let persistence =
            match QuotaPersistence::spawn(storage.clone(), source.clone(), inner.clone()) {
                Ok(owner) => Arc::new(owner),
                Err(error) => return finish_cleanup(Err(error), storage.close_lease()),
            };
        Ok(Self {
            inner,
            persistence: Some(persistence),
            stopping: AtomicBool::new(false),
        })
    }

    /// Fence admission before checking actual leases. A retry after the last
    /// registration returns consumes the original worker, then the native lease.
    pub(crate) fn shutdown(&self) -> io::Result<()> {
        let book = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.stopping.store(true, Ordering::Release);
        if book.states.values().any(|state| state.references != 0) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "principal quota registrations are still active",
            ));
        }
        drop(book);
        self.persistence
            .as_ref()
            .map_or(Ok(()), |owner| owner.shutdown())
    }

    pub(crate) fn retirement_complete(&self) -> bool {
        if !self.stopping.load(Ordering::Acquire)
            || self
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .states
                .values()
                .any(|state| state.references != 0)
        {
            return false;
        }
        self.persistence.as_ref().is_none_or(|owner| {
            let worker = owner
                .worker
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let joined = worker.original.is_none() && worker.joined.is_some();
            drop(worker);
            joined
                && owner.source.confirm_quota_storage_closed().is_ok()
                && owner.source.confirm_original_io_closed().is_ok()
        })
    }

    pub(crate) fn acquire(
        &self,
        auth: Option<&SessionAuth>,
    ) -> Result<Option<PrincipalQuotaRegistration>, EngineError> {
        let Some(auth) = auth else {
            return Ok(None);
        };
        let Some(initial_bytes) = auth.quota_remaining_bytes else {
            return Ok(None);
        };
        if let Some(error) = self.persistence_error() {
            return Err(EngineError::AdmissionDenied {
                reason: format!("principal quota persistence is unavailable: {error}"),
            });
        }
        let principal_key =
            auth.principal_key
                .as_deref()
                .ok_or_else(|| EngineError::AdmissionDenied {
                    reason: "quota-limited session has no principal identity".to_owned(),
                })?;
        let key = QuotaKey {
            principal_key: principal_key.to_owned(),
            policy_revision: auth.policy_revision,
            initial_bytes,
        };
        let mut book = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if self.stopping.load(Ordering::Acquire) {
            return Err(EngineError::AdmissionDenied {
                reason: "principal quota persistence is stopping".to_owned(),
            });
        }
        let changed = book.current.get(principal_key) != Some(&key);
        if changed {
            book.current.insert(principal_key.to_owned(), key.clone());
            book.states.retain(|candidate, state| {
                candidate.principal_key != principal_key
                    || candidate == &key
                    || state.references > 0
            });
        }
        let state = book.states.entry(key.clone()).or_insert(QuotaState {
            remaining_bytes: initial_bytes,
            references: 0,
        });
        if state.remaining_bytes == 0 {
            return Err(EngineError::AdmissionDenied {
                reason: format!("principal `{principal_key}` exhausted its traffic quota"),
            });
        }
        state.references += 1;
        drop(book);
        if changed {
            self.mark_dirty();
        }
        Ok(Some(PrincipalQuotaRegistration {
            registry: self.inner.clone(),
            persistence: self.persistence.clone(),
            key,
        }))
    }

    pub(crate) fn forget(&self, principal_key: &str) {
        let mut book = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        book.current.remove(principal_key);
        book.states
            .retain(|key, state| key.principal_key != principal_key || state.references > 0);
        drop(book);
        self.mark_dirty();
    }

    fn mark_dirty(&self) {
        if let Some(persistence) = &self.persistence {
            persistence.mark_dirty();
        }
    }

    fn persistence_error(&self) -> Option<String> {
        self.persistence.as_ref().and_then(|persistence| {
            persistence
                .last_error
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .map(ToString::to_string)
        })
    }
}

impl Drop for PrincipalQuotaRegistry {
    fn drop(&mut self) {
        // Active registrations retain the same actual worker owner and book.
        // Drop never substitutes for the explicit successful shutdown receipt.
        if let Err(error) = self.shutdown() {
            if error.kind() != io::ErrorKind::WouldBlock {
                error!(%error, "quota shutdown failed during fallback cleanup");
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistration {
    registry: Arc<Mutex<QuotaBook>>,
    persistence: Option<Arc<QuotaPersistence>>,
    key: QuotaKey,
}

impl PrincipalQuotaRegistration {
    pub(crate) fn consume(&self, bytes: u64) -> Option<String> {
        if bytes == 0 {
            return None;
        }
        let mut book = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let state = book.states.get_mut(&self.key)?;
        let previous = state.remaining_bytes;
        state.remaining_bytes = state.remaining_bytes.saturating_sub(bytes);
        let exhausted = previous > 0 && state.remaining_bytes == 0;
        drop(book);
        if let Some(owner) = &self.persistence {
            owner.mark_dirty();
        }
        exhausted.then(|| self.key.principal_key.clone())
    }
}

impl Drop for PrincipalQuotaRegistration {
    fn drop(&mut self) {
        let mut book = self
            .registry
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let is_current = book.current.get(&self.key.principal_key) == Some(&self.key);
        let Some(state) = book.states.get_mut(&self.key) else {
            return;
        };
        state.references = state.references.saturating_sub(1);
        if state.references == 0 && !is_current {
            book.states.remove(&self.key);
        }
    }
}

impl QuotaPersistence {
    fn spawn(
        storage: Arc<QuotaStorage>,
        source: Arc<ConfigurationSource>,
        book: Arc<Mutex<QuotaBook>>,
    ) -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let last_error = Arc::new(Mutex::new(None));
        let worker_error = last_error.clone();
        let worker_storage = storage.clone();
        let worker = std::thread::Builder::new().name("zero-quota-state".to_owned()).spawn(move || {
            while let Ok(message) = receiver.recv() {
                let mut shutdown = matches!(message, PersistenceMessage::Shutdown);
                if !shutdown {
                    std::thread::sleep(Duration::from_millis(50));
                    loop {
                        match receiver.try_recv() {
                            Ok(PersistenceMessage::Dirty) => {}
                            Ok(PersistenceMessage::Shutdown) => shutdown = true,
                            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                        }
                    }
                }
                let result = serde_json::to_vec(&snapshot_book(&book))
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
                    .and_then(|bytes| worker_storage.replace(&bytes));
                match result {
                    Ok(()) => *worker_error.lock().unwrap_or_else(|error| error.into_inner()) = None,
                    Err(error) => {
                        let error = Arc::new(error);
                        error!(path = %worker_storage.path().display(), %error, "failed to persist principal quota state");
                        *worker_error.lock().unwrap_or_else(|error| error.into_inner()) = Some(error.clone());
                        if shutdown { return Err(retained(&error)); }
                    }
                }
                if shutdown { break; }
            }
            Ok(())
        })?;
        info!(path = %storage.path().display(), "principal quota recovery enabled");
        Ok(Self {
            storage,
            source,
            sender,
            worker: Mutex::new(Worker {
                original: Some(worker),
                joined: None,
            }),
            last_error,
        })
    }
    fn mark_dirty(&self) {
        mark_sender_dirty(Some(&self.sender));
    }
    fn shutdown(&self) -> io::Result<()> {
        let mut state = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.joined.is_none() {
            // Only this actual owner can consume the original std thread.
            let _ = self.sender.send(PersistenceMessage::Shutdown);
            let joined = state
                .original
                .take()
                .expect("quota original worker is retained until joined")
                .join();
            state.joined = Some(match joined {
                Ok(result) => result.map_err(Arc::new),
                Err(payload) => Err(Arc::new(io::Error::other(WorkerPanic(Mutex::new(payload))))),
            });
        }
        let joined = state
            .joined
            .as_ref()
            .expect("actual worker joined")
            .as_ref()
            .map(|_| ())
            .map_err(retained);
        // Even a failed worker has exited; its original native lease can now
        // close. Keep both the typed worker result and matching close failure.
        let cleanup = self.storage.close_lease();
        if cleanup.is_err() {
            self.source.retain_quota_cleanup();
        }
        finish_cleanup(joined, cleanup)
    }
}
impl Drop for QuotaPersistence {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            error!(%error, "quota fallback cleanup failed");
        }
    }
}

fn mark_sender_dirty(sender: Option<&SyncSender<PersistenceMessage>>) {
    let Some(sender) = sender else { return };
    match sender.try_send(PersistenceMessage::Dirty) {
        Ok(()) | Err(TrySendError::Full(_)) => {}
        Err(TrySendError::Disconnected(_)) => {}
    }
}

fn snapshot_book(book: &Arc<Mutex<QuotaBook>>) -> PersistedQuotaBook {
    let book = book.lock().unwrap_or_else(|error| error.into_inner());
    let balances = book
        .current
        .values()
        .filter_map(|key| {
            book.states.get(key).map(|state| PersistedQuotaBalance {
                key: key.clone(),
                remaining_bytes: state.remaining_bytes,
            })
        })
        .collect();
    PersistedQuotaBook {
        version: 1,
        balances,
    }
}

fn read_book(storage: &QuotaStorage) -> io::Result<QuotaBook> {
    let Some(persisted) = storage.read_with(|reader| decode_persisted_book(reader))? else {
        return Ok(QuotaBook::default());
    };
    let mut book = QuotaBook::default();
    for balance in persisted.balances {
        book.current
            .insert(balance.key.principal_key.clone(), balance.key.clone());
        book.states.insert(
            balance.key,
            QuotaState {
                remaining_bytes: balance.remaining_bytes,
                references: 0,
            },
        );
    }
    Ok(book)
}

pub fn inspect_principal_quota_state(
    config: &zero_config::RuntimeConfig,
) -> Option<PrincipalQuotaStateReport> {
    // Explicit standalone inspection has no already loaded application source.
    // Its source is nevertheless retained by the same startup close barrier.
    inspect_principal_quota_state_with_source(config, &Arc::new(ConfigurationSource::default()))
}

/// Inspect through the source that loaded this application's configuration.
/// CLI connector-state uses this entry so an unknown document/quota original
/// remains with that same owner and fences further reads; no fallback source.
pub fn inspect_principal_quota_state_with_source(
    config: &zero_config::RuntimeConfig,
    source: &Arc<ConfigurationSource>,
) -> Option<PrincipalQuotaStateReport> {
    let configured = config.runtime.principal_quota_state_path.as_deref()?;
    let path = {
        let path = PathBuf::from(configured);
        if path.is_absolute() {
            path
        } else {
            config
                .source_dir()
                .unwrap_or_else(|| Path::new("."))
                .join(path)
        }
    };
    let (bytes, decoded) =
        match source.read_quota_snapshot(&path, |reader| decode_persisted_book(reader)) {
            Ok(snapshot) => snapshot,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Some(PrincipalQuotaStateReport {
                    format: "zero.engine.principal-quota.v1",
                    path: path.display().to_string(),
                    status: PrincipalQuotaStateStatus::Missing,
                    bytes: 0,
                    balances: None,
                    error: None,
                });
            }
            Err(error) => {
                return Some(PrincipalQuotaStateReport {
                    format: "zero.engine.principal-quota.v1",
                    path: path.display().to_string(),
                    status: PrincipalQuotaStateStatus::Incompatible,
                    bytes: 0,
                    balances: None,
                    error: Some(format!("read failed: {error}")),
                });
            }
        };
    match decoded {
        Ok(book) => Some(PrincipalQuotaStateReport {
            format: "zero.engine.principal-quota.v1",
            path: path.display().to_string(),
            status: PrincipalQuotaStateStatus::Ready,
            bytes,
            balances: Some(book.balances.len() as u64),
            error: None,
        }),
        Err(error) => Some(PrincipalQuotaStateReport {
            format: "zero.engine.principal-quota.v1",
            path: path.display().to_string(),
            status: PrincipalQuotaStateStatus::Incompatible,
            bytes,
            balances: None,
            error: Some(error.to_string()),
        }),
    }
}

fn decode_persisted_book(reader: impl Read) -> io::Result<PersistedQuotaBook> {
    let persisted = serde_json::from_reader::<_, PersistedQuotaBook>(reader)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if persisted.version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported principal quota state version {}",
                persisted.version
            ),
        ));
    }
    let mut principals = HashSet::with_capacity(persisted.balances.len());
    for balance in &persisted.balances {
        if !principals.insert(&balance.key.principal_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "duplicate principal quota balance for `{}`",
                    balance.key.principal_key
                ),
            ));
        }
    }
    Ok(persisted)
}

#[cfg(test)]
#[path = "tests/quota_lifecycle.rs"]
mod lifecycle_tests;
