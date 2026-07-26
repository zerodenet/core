//! Principal traffic quota accounting and durable recovery state.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{error, info};
use zero_core::SessionAuth;

use crate::EngineError;

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

#[derive(Debug)]
struct QuotaPersistence {
    _state_lease: QuotaStateLease,
    sender: SyncSender<PersistenceMessage>,
    worker: Mutex<Option<JoinHandle<()>>>,
    last_error: Arc<Mutex<Option<String>>>,
}

#[derive(Debug)]
struct QuotaStateLease {
    _file: File,
}

impl QuotaStateLease {
    fn acquire(path: &Path) -> io::Result<Self> {
        let lock_path = quota_lock_path(path);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "principal quota state `{}` is already owned by another Zero process (lock `{}`)",
                    path.display(),
                    lock_path.display()
                ),
            )),
            Err(TryLockError::Error(error)) => Err(io::Error::new(
                error.kind(),
                format!(
                    "failed to acquire principal quota state lock `{}` for `{}`: {error}",
                    lock_path.display(),
                    path.display()
                ),
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistry {
    inner: Arc<Mutex<QuotaBook>>,
    persistence: Option<QuotaPersistence>,
}

impl Default for PrincipalQuotaRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QuotaBook::default())),
            persistence: None,
        }
    }
}

impl PrincipalQuotaRegistry {
    pub(crate) fn open(state_path: Option<PathBuf>) -> io::Result<Self> {
        let Some(state_path) = state_path else {
            return Ok(Self::default());
        };
        let state_lease = QuotaStateLease::acquire(&state_path)?;
        let book = read_book(&state_path)?;
        let inner = Arc::new(Mutex::new(book));
        let persistence = QuotaPersistence::spawn(state_path, state_lease, Arc::downgrade(&inner))?;
        Ok(Self {
            inner,
            persistence: Some(persistence),
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
            registry: Arc::downgrade(&self.inner),
            persistence: self
                .persistence
                .as_ref()
                .map(|persistence| persistence.sender.clone()),
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
                .clone()
        })
    }
}

impl Drop for PrincipalQuotaRegistry {
    fn drop(&mut self) {
        // Drop and join the persistence worker while `inner` is still alive
        // so its final checkpoint can upgrade the weak book reference.
        if let Some(persistence) = self.persistence.take() {
            drop(persistence);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistration {
    registry: Weak<Mutex<QuotaBook>>,
    persistence: Option<SyncSender<PersistenceMessage>>,
    key: QuotaKey,
}

impl PrincipalQuotaRegistration {
    pub(crate) fn consume(&self, bytes: u64) -> Option<String> {
        if bytes == 0 {
            return None;
        }
        let registry = self.registry.upgrade()?;
        let mut book = registry.lock().unwrap_or_else(|error| error.into_inner());
        let state = book.states.get_mut(&self.key)?;
        let previous = state.remaining_bytes;
        state.remaining_bytes = state.remaining_bytes.saturating_sub(bytes);
        let exhausted = previous > 0 && state.remaining_bytes == 0;
        drop(book);
        mark_sender_dirty(self.persistence.as_ref());
        exhausted.then(|| self.key.principal_key.clone())
    }
}

impl Drop for PrincipalQuotaRegistration {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut book = registry.lock().unwrap_or_else(|error| error.into_inner());
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
        path: PathBuf,
        state_lease: QuotaStateLease,
        book: Weak<Mutex<QuotaBook>>,
    ) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        let last_error = Arc::new(Mutex::new(None));
        let worker_error = last_error.clone();
        let worker_path = path.clone();
        let worker = std::thread::Builder::new()
            .name("zero-quota-state".to_owned())
            .spawn(move || {
                while let Ok(message) = receiver.recv() {
                    let mut shutdown = matches!(message, PersistenceMessage::Shutdown);
                    if !shutdown {
                        // Coalesce high-frequency traffic accounting into one
                        // crash-recovery checkpoint without blocking relay I/O.
                        std::thread::sleep(Duration::from_millis(50));
                        loop {
                            match receiver.try_recv() {
                                Ok(PersistenceMessage::Dirty) => {}
                                Ok(PersistenceMessage::Shutdown) => shutdown = true,
                                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                            }
                        }
                    }
                    let Some(book) = book.upgrade() else {
                        break;
                    };
                    let snapshot = snapshot_book(&book);
                    match write_book(&worker_path, &snapshot) {
                        Ok(()) => {
                            *worker_error
                                .lock()
                                .unwrap_or_else(|error| error.into_inner()) = None;
                        }
                        Err(write_error) => {
                            let message = write_error.to_string();
                            error!(path = %worker_path.display(), error = %message, "failed to persist principal quota state");
                            *worker_error
                                .lock()
                                .unwrap_or_else(|error| error.into_inner()) = Some(message);
                        }
                    }
                    if shutdown {
                        break;
                    }
                }
            })?;
        info!(path = %path.display(), "principal quota recovery enabled");
        Ok(Self {
            _state_lease: state_lease,
            sender,
            worker: Mutex::new(Some(worker)),
            last_error,
        })
    }

    fn mark_dirty(&self) {
        mark_sender_dirty(Some(&self.sender));
    }
}

impl Drop for QuotaPersistence {
    fn drop(&mut self) {
        let _ = self.sender.send(PersistenceMessage::Dirty);
        let _ = self.sender.send(PersistenceMessage::Shutdown);
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = worker.join();
        }
    }
}

fn quota_lock_path(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    PathBuf::from(lock_path)
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

fn read_book(path: &Path) -> io::Result<QuotaBook> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(QuotaBook::default()),
        Err(error) => return Err(error),
    };
    let persisted = decode_persisted_book(&raw)?;
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
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
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
    match decode_persisted_book(&raw) {
        Ok(book) => Some(PrincipalQuotaStateReport {
            format: "zero.engine.principal-quota.v1",
            path: path.display().to_string(),
            status: PrincipalQuotaStateStatus::Ready,
            bytes: raw.len() as u64,
            balances: Some(book.balances.len() as u64),
            error: None,
        }),
        Err(error) => Some(PrincipalQuotaStateReport {
            format: "zero.engine.principal-quota.v1",
            path: path.display().to_string(),
            status: PrincipalQuotaStateStatus::Incompatible,
            bytes: raw.len() as u64,
            balances: None,
            error: Some(error.to_string()),
        }),
    }
}

fn decode_persisted_book(raw: &[u8]) -> io::Result<PersistedQuotaBook> {
    let persisted = serde_json::from_slice::<PersistedQuotaBook>(raw)
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

fn write_book(path: &Path, book: &PersistedQuotaBook) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let raw = serde_json::to_vec(book)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&raw)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
