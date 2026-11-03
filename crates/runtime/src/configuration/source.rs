//! Atomic deployment source persistence and the minimal crash-recovery record.
//! Records contain digests and phase facts, never configuration credentials.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};
use zero_config::RuntimeConfig;

#[derive(Debug, Clone, Default)]
pub struct SourceWriteObservation {
    pub successful_writes: u64,
    pub writable: Option<bool>,
    pub observed_at_unix_ms: Option<u64>,
    pub reason: Option<String>,
}
#[derive(Debug, Default)]
pub struct ConfigurationSource {
    path: Option<PathBuf>,
    observation: Mutex<SourceWriteObservation>,
}
#[derive(Serialize, Deserialize)]
struct RecoveryRecord {
    schema: u8,
    attempt: String,
    old_digest: Option<String>,
    candidate_digest: String,
    persistent: bool,
    completed_phase: String,
    compensation_identity: String,
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
impl ConfigurationSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            observation: Mutex::default(),
        }
    }
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
    pub fn observation(&self) -> SourceWriteObservation {
        self.observation
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    /// Reconcile an interrupted durable attempt against the actual source.
    /// Before replacement the old source wins; after replacement the candidate
    /// wins. Neither case asserts the old caller received an acknowledgement.
    pub fn verify_recovery(path: &Path) -> io::Result<()> {
        let record_path = sidecar(path, ".zero-recovery");
        let raw = match std::fs::read(&record_path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        let record: RecoveryRecord = serde_json::from_slice(&raw).map_err(io::Error::other)?;
        if record.schema != 1
            || !record.persistent
            || !matches!(record.completed_phase.as_str(), "ready" | "persisted")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported configuration recovery record",
            ));
        }
        let actual = std::fs::read(path).map(|bytes| digest(&bytes))?;
        if actual != record.candidate_digest && record.old_digest.as_ref() != Some(&actual) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "configuration source does not match its recovery record",
            ));
        }
        Ok(())
    }
    /// Call only after the owning operation has observed candidate Ready.
    /// Engine-only callers have no proxy resources and retain their narrower
    /// synchronous preparation/persistence contract.
    pub fn persist_ready(&self, config: &RuntimeConfig, identity: &str) -> io::Result<()> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        // Serialize before touching the only last-good deployment source.
        let candidate = serde_json::to_vec_pretty(config).map_err(io::Error::other)?;
        let mut observation = self
            .observation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let result = self.persist(path, &candidate, identity);
        observation.observed_at_unix_ms = Some(now_ms());
        match &result {
            Ok(()) => {
                observation.successful_writes = observation.successful_writes.wrapping_add(1);
                observation.writable = Some(true);
                observation.reason = None;
            }
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                observation.writable = Some(false);
                observation.reason = Some("source_write_permission_denied".into());
            }
            Err(_) => {
                observation.writable = None;
                observation.reason = Some("source_write_failed".into());
            }
        }
        result
    }
    fn persist(&self, path: &Path, candidate: &[u8], identity: &str) -> io::Result<()> {
        let previous = match std::fs::read(path) {
            Ok(raw) => Some(raw),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let permissions = std::fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions());
        if let Some(previous) = previous.as_ref() {
            atomic_write(
                &sidecar(path, ".zero-last-good"),
                previous,
                permissions.clone(),
            )?;
        }
        let mut record = RecoveryRecord {
            schema: 1,
            attempt: format!(
                "{identity}:{}:{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
            old_digest: previous.as_ref().map(|bytes| digest(bytes)),
            candidate_digest: digest(candidate),
            persistent: true,
            completed_phase: "ready".into(),
            compensation_identity: identity.into(),
        };
        let record_path = sidecar(path, ".zero-recovery");
        atomic_write(
            &record_path,
            &serde_json::to_vec(&record).map_err(io::Error::other)?,
            permissions.clone(),
        )?;
        if let Err(error) = atomic_write(path, candidate, permissions.clone()) {
            // Rename can succeed before a directory-sync error. Inspect the
            // actual file and restore the original before reporting failure.
            match std::fs::read(path) {
                Ok(actual) if digest(&actual) == record.candidate_digest => {
                    if let Some(previous) = previous.as_ref() {
                        atomic_write(path, previous, permissions.clone()).map_err(|restore| io::Error::other(format!("source replacement failed: {error}; original source recovery failed: {restore}")))?;
                    }
                }
                Ok(actual) if record.old_digest.as_ref() == Some(&digest(&actual)) => {}
                Err(missing) if missing.kind() == io::ErrorKind::NotFound && previous.is_none() => {
                }
                Ok(_) | Err(_) => return Err(io::Error::other(format!(
                    "source replacement failed: {error}; actual source outcome remains unconfirmed"
                ))),
            }
            return Err(error);
        }
        record.completed_phase = "persisted".into();
        // If this final record update fails, inspect the source on restart. The
        // ready record already names both possible atomic replacement results.
        // Restore the old deployment before returning a pre-commit failure.
        if let Err(error) = atomic_write(
            &record_path,
            &serde_json::to_vec(&record).map_err(io::Error::other)?,
            permissions.clone(),
        ) {
            if let Some(previous) = previous {
                atomic_write(path, &previous, permissions)?;
            }
            return Err(error);
        }
        Ok(())
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn atomic_write(
    path: &Path,
    bytes: &[u8],
    permissions: Option<std::fs::Permissions>,
) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    if let Some(permissions) = permissions {
        temporary.as_file().set_permissions(permissions)?;
    }
    temporary.write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
