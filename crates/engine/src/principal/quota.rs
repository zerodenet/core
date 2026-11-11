//! Principal traffic quota accounting and durable recovery state.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tracing::{error, info};
use zero_core::SessionAuth;

use crate::EngineError;
use zero_runtime::configuration::{
    ConfigurationSource, OwnedQuotaStorage, QuotaMemoryBudget, QuotaMemoryCredit, QuotaStorage,
};
use zero_traits::packet_metadata::PacketMetadataLayout;
mod memory;
mod read;
mod snapshot;
use memory::{BookOwner, Current};

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
    _key: Option<QuotaMemoryCredit>,
}

#[derive(Debug, Default)]
struct QuotaBook {
    current: HashMap<String, Current>,
    states: HashMap<QuotaKey, QuotaState>,
    memory: Option<QuotaMemoryBudget>,
    persistent: bool,
    current_storage: Option<QuotaMemoryCredit>,
    state_storage: Option<QuotaMemoryCredit>,
    promise: memory::Promise,
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
struct QuotaPersistence {
    storage: OwnedQuotaStorage,
    source: Arc<ConfigurationSource>,
}
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
#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistry {
    inner: BookOwner,
    persistence: Option<Arc<QuotaPersistence>>,
    stopping: AtomicBool,
}

impl Default for PrincipalQuotaRegistry {
    fn default() -> Self {
        Self {
            inner: BookOwner::Library(Arc::new(Mutex::new(QuotaBook::default()))),
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
        Self::open_with_runtime(
            state_path,
            source,
            &zero_config::RuntimeOptionsConfig::default(),
        )
    }
    pub(crate) fn open_with_runtime(
        state_path: Option<PathBuf>,
        source: Arc<ConfigurationSource>,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> io::Result<Self> {
        let memory = source.configure_initial_quota_memory(runtime)?;
        let Some(state_path) = state_path else {
            return Ok(Self {
                inner: BookOwner::paid(memory::new_book(&memory, false)?, &memory)?,
                persistence: None,
                stopping: AtomicBool::new(false),
            });
        };
        let storage = source.open_owned_quota_storage(state_path, &memory)?;
        let book = match read_book(&storage, &memory) {
            Ok(book) => book,
            Err(error) => return finish_cleanup(Err(error), storage.close_lease()),
        };
        let inner = match BookOwner::paid(book, &memory) {
            Ok(value) => value,
            Err(error) => return finish_cleanup(Err(error), storage.close_lease()),
        };
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
        self.shutdown_received(true)
    }
    fn fence_shutdown(&self) -> io::Result<()> {
        let book = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.stopping.store(true, Ordering::Release);
        if book.states.values().any(|state| state.references != 0) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "principal quota registrations are still active",
            ));
        }
        Ok(())
    }

    fn shutdown_received(&self, reported: bool) -> io::Result<()> {
        self.fence_shutdown()?;
        self.persistence
            .as_ref()
            .map_or(Ok(()), |owner| owner.shutdown(reported))
    }

    pub(crate) async fn shutdown_async(&self) -> io::Result<()> {
        self.fence_shutdown()?;
        match &self.persistence {
            Some(owner) => owner.storage.shutdown_writer_async().await,
            None => Ok(()),
        }
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
            owner.storage.writer_retirement_complete()
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
        let memory = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .memory
            .clone();
        let key_credit = memory
            .as_ref()
            .map(|memory| memory.reserve(memory::key_storage(principal_key.len(), 1)?))
            .transpose()?;
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
        let changed = book.current.get(principal_key).map(|current| &current.key) != Some(&key);
        let new_current = !book.current.contains_key(principal_key);
        let new_state = !book.states.contains_key(&key);
        let strings = if changed {
            memory
                .as_ref()
                .map(|memory| memory.reserve(memory::key_storage(principal_key.len(), 2)?))
                .transpose()?
        } else {
            None
        };
        let state_string = if new_state {
            memory
                .as_ref()
                .map(|memory| memory.reserve(memory::key_storage(principal_key.len(), 1)?))
                .transpose()?
        } else {
            None
        };
        let promise_layout = if new_current && book.persistent {
            memory::snapshot_layout(&key)?
        } else {
            PacketMetadataLayout::new(0, 0)
        };
        let promise_credit = book
            .promise
            .prepare_delta(memory.as_ref(), promise_layout)?;
        let QuotaBook {
            current,
            states,
            current_storage,
            state_storage,
            ..
        } = &mut *book;
        let old_current_storage = memory::grow(
            current,
            current_storage,
            memory.as_ref(),
            usize::from(new_current),
        )?;
        let old_state_storage = memory::grow(
            states,
            state_storage,
            memory.as_ref(),
            usize::from(new_state),
        )?;
        let unused_promise = book.promise.add(promise_layout, promise_credit);
        if changed {
            let promise = book
                .current
                .get(principal_key)
                .map_or(promise_layout, |current| current.promise);
            book.current.insert(
                principal_key.to_owned(),
                Current {
                    key: key.clone(),
                    promise,
                    _strings: strings,
                },
            );
            book.states.retain(|candidate, state| {
                candidate.principal_key != principal_key
                    || candidate == &key
                    || state.references > 0
            });
        }
        if new_state {
            book.states.insert(
                key.clone(),
                QuotaState {
                    remaining_bytes: initial_bytes,
                    references: 0,
                    _key: state_string,
                },
            );
        }
        let state = book
            .states
            .get_mut(&key)
            .expect("quota state admitted above");
        if state.remaining_bytes == 0 {
            return Err(EngineError::AdmissionDenied {
                reason: format!("principal `{principal_key}` exhausted its traffic quota"),
            });
        }
        state.references += 1;
        drop(book);
        drop(old_current_storage);
        drop(old_state_storage);
        drop(unused_promise);
        if changed {
            self.mark_dirty();
        }
        Ok(Some(PrincipalQuotaRegistration {
            registry: self.inner.clone(),
            persistence: self.persistence.clone(),
            key,
            _key_credit: key_credit,
        }))
    }

    pub(crate) fn forget(&self, principal_key: &str) {
        let mut book = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(current) = book.current.remove(principal_key) {
            book.promise.remove(current.promise);
        }
        book.states
            .retain(|key, state| key.principal_key != principal_key || state.references > 0);
        let unused = book.promise.trim();
        drop(book);
        drop(unused);
        self.mark_dirty();
    }

    fn mark_dirty(&self) {
        if let Some(persistence) = &self.persistence {
            persistence.mark_dirty();
        }
    }

    fn persistence_error(&self) -> Option<String> {
        self.persistence
            .as_ref()
            .and_then(|owner| owner.storage.writer_error().map(|error| error.to_string()))
    }
}

impl Drop for PrincipalQuotaRegistry {
    fn drop(&mut self) {
        // Active registrations retain the same actual worker owner and book.
        // Drop never substitutes for the explicit successful shutdown receipt.
        if let Err(error) = self.shutdown_received(false) {
            if error.kind() != io::ErrorKind::WouldBlock {
                error!(%error, "quota shutdown failed during fallback cleanup");
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct PrincipalQuotaRegistration {
    registry: BookOwner,
    persistence: Option<Arc<QuotaPersistence>>,
    key: QuotaKey,
    _key_credit: Option<QuotaMemoryCredit>,
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
        let is_current = book
            .current
            .get(&self.key.principal_key)
            .map(|current| &current.key)
            == Some(&self.key);
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
        storage: OwnedQuotaStorage,
        source: Arc<ConfigurationSource>,
        book: BookOwner,
    ) -> io::Result<Self> {
        let original = storage.downgrade();
        // Retain the one accounting book for the final accepted flush. It owns
        // only domain memory; the callback has no strong Source/IO/Scope edge.
        storage.prepare_writer(move || {
            let storage = original.upgrade().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "quota storage retired")
            })?;
            snapshot::encode(&book).and_then(|frame| storage.replace(&frame.bytes))
        })?;
        let owner = Self { storage, source };
        info!(path = %owner.storage.path().display(), "principal quota recovery enabled");
        Ok(owner)
    }
    fn mark_dirty(&self) {
        if let Err(error) = self.storage.mark_writer_dirty() {
            error!(%error, "quota writer could not accept dirty state");
        }
    }
    fn shutdown(&self, reported: bool) -> io::Result<()> {
        let result = self.storage.shutdown_writer(reported);
        if !reported && result.is_err() && !self.storage.writer_retirement_complete() {
            self.source.retain_quota_cleanup();
        }
        result
    }
}
impl Drop for QuotaPersistence {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown(false) {
            if error.kind() != io::ErrorKind::WouldBlock {
                error!(%error, "quota fallback cleanup failed");
            }
        }
    }
}

fn read_book(storage: &QuotaStorage, memory: &QuotaMemoryBudget) -> io::Result<QuotaBook> {
    let decoded = storage.read_seekable_with(|reader| read::decode(reader, memory))?;
    let mut book = memory::new_book(memory, true)?;
    let Some(decoded) = decoded else {
        return Ok(book);
    };
    for balance in &decoded.value.balances {
        let length = balance.key.principal_key.len();
        let current_strings = memory.reserve(memory::key_storage(length, 2)?)?;
        let state_string = memory.reserve(memory::key_storage(length, 1)?)?;
        let promise = memory::snapshot_layout(&balance.key)?;
        let promise_credit = book.promise.prepare_delta(Some(memory), promise)?;
        let old_current = memory::grow(
            &mut book.current,
            &mut book.current_storage,
            Some(memory),
            1,
        )?;
        let old_states = memory::grow(&mut book.states, &mut book.state_storage, Some(memory), 1)?;
        drop(old_current);
        drop(old_states);
        let empty = book.promise.add(promise, promise_credit);
        drop(empty);
        book.current.insert(
            balance.key.principal_key.clone(),
            Current {
                key: balance.key.clone(),
                promise,
                _strings: Some(current_strings),
            },
        );
        book.states.insert(
            balance.key.clone(),
            QuotaState {
                remaining_bytes: balance.remaining_bytes,
                references: 0,
                _key: Some(state_string),
            },
        );
    }
    // Includes the actual original typed DTO and serde/duplicate workspace.
    // Only after all final maps/promises exist can their separate credit return.
    drop(decoded);
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
    let memory = match source.quota_memory(&config.runtime) {
        Ok(memory) => memory,
        Err(error) => {
            return Some(PrincipalQuotaStateReport {
                format: "zero.engine.principal-quota.v1",
                path: path.display().to_string(),
                status: PrincipalQuotaStateStatus::Incompatible,
                bytes: 0,
                balances: None,
                error: Some(format!("quota storage unavailable: {error}")),
            })
        }
    };
    let (bytes, decoded) = match source
        .inspect_seekable_file_snapshot(&path, |_length, reader| read::decode(reader, &memory))
    {
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
            balances: Some(book.value.balances.len() as u64),
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
