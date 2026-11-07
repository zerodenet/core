//! Atomic source persistence with retained original bytes and explicit outcomes.
//! The same accepted host operation drives persist, commit, or compensation.
mod cache;
mod document;
mod quota;
mod snapshot;
pub use snapshot::ConfigurationSnapshotReader;
pub use quota::QuotaStorage;
mod embedded;
mod files;
mod model;
mod persistence;
mod transaction;
pub use cache::{RuleSetCacheDownload, MAX_RULE_SET_CACHE_BYTES};
pub use model::{SourceDisposition, SourcePersistError, SourceWriteObservation};
use std::{
    io,
    path::{Path, PathBuf},
    sync::Mutex,
};
use transaction::{PendingSource, RecoveryRecord};
use zero_config::RuntimeConfig;

#[derive(Default)]
struct SourceState {
    observation: SourceWriteObservation,
    pending: Option<PendingSource>,
}
#[derive(Default)]
pub struct ConfigurationSource {
    path: Option<PathBuf>,
    state: Mutex<SourceState>,
    cache: cache::CacheState,
    pub(super) io: super::io::ConfigurationIo,
    quota: quota::QuotaSlot,
}
impl std::fmt::Debug for ConfigurationSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigurationSource")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}
impl ConfigurationSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Some(path.into()),
            state: Mutex::default(),
            io: super::io::ConfigurationIo::default(),
            cache: Default::default(),
            quota: Default::default(),
        }
    }
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
    pub fn observation(&self) -> SourceWriteObservation {
        let mut observation = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .observation
            .clone();
        if self.ensure_resource_io_available().is_err() {
            observation.disposition = SourceDisposition::Unknown;
            observation.writable = None;
            observation.reason = Some("source_file_cleanup_unconfirmed".into());
        }
        observation
    }
    /// An independent final observation after the accepted task's real joins.
    /// This performs no file open, persistence, recovery, or replacement.
    pub fn confirm_original_io_closed(&self) -> io::Result<()> {
        self.ensure_resource_io_available()?;
        super::startup::confirm_closed()
    }
    pub(super) fn has_retained_cache(&self) -> bool {
        cache::is_retained(&self.cache)
    }
    pub(super) fn ensure_resource_io_available(&self) -> io::Result<()> {
        self.quota_io_available()?;
        cache::ensure_idle(&self.cache)?;
        self.io.ensure_available()
    }
    pub fn verify_recovery(path: &Path) -> io::Result<()> {
        let source = std::sync::Arc::new(Self::new(path));
        source.verify_initial_recovery(path)
    }
    fn verify_recovery_owned(&self, path: &Path) -> io::Result<()> {
        let Some(raw) = files::read_recovery(path, &self.io)? else {
            return Ok(());
        };
        let record: RecoveryRecord = serde_json::from_slice(&raw).map_err(io::Error::other)?;
        if record.schema != 1
            || !record.persistent
            || !matches!(
                record.completed_phase.as_str(),
                "ready" | "persisted" | "restored" | "committed"
            )
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported configuration recovery record",
            ));
        }
        // Completed records have no authority over a later operator edit.
        if record.completed_phase == "committed" {
            return Ok(());
        }
        let actual = files::read_optional(path, &self.io)?
            .as_ref()
            .map(|bytes| files::digest(bytes));
        if actual.as_ref() != Some(&record.candidate_digest) && actual != record.old_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "configuration source does not match its recovery record",
            ));
        }
        Ok(())
    }
    /// A newly constructed kernel has consumed and validated the authoritative
    /// old/candidate source named by an interrupted attempt. It has a new core
    /// instance and never claims the previous caller received an ACK.
    pub fn complete_recovered_boot(self: &std::sync::Arc<Self>) -> io::Result<()> {
        super::startup::with_source(self.clone(), || self.complete_recovered_boot_owned())?
    }
    fn complete_recovered_boot_owned(&self) -> io::Result<()> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        self.verify_recovery_owned(path)?;
        let record_path = files::sidecar(path, ".zero-recovery");
        let Some(raw) = files::read_recovery(path, &self.io)? else {
            return Ok(());
        };
        let mut record: RecoveryRecord = serde_json::from_slice(&raw).map_err(io::Error::other)?;
        record.completed_phase = "committed".into();
        let permissions = std::fs::metadata(path)?.permissions();
        files::atomic_write(
            &record_path,
            &serde_json::to_vec(&record).map_err(io::Error::other)?,
            Some(permissions),
            &self.io,
        )?;
        files::remove_sidecars(path, &self.io)
    }
}
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
#[path = "../tests/configuration_source.rs"]
mod tests;
