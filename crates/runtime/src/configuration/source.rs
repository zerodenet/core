//! Atomic source persistence with retained original bytes and explicit outcomes.
//! The same accepted host operation drives persist, commit, or compensation.
mod files;
mod model;
mod transaction;
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
        }
    }
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
    pub fn observation(&self) -> SourceWriteObservation {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .observation
            .clone()
    }
    pub fn verify_recovery(path: &Path) -> io::Result<()> {
        let Some(raw) = files::read_optional(&files::sidecar(path, ".zero-recovery"))? else {
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
        let actual = files::read_optional(path)?
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
    pub fn complete_recovered_boot(&self) -> io::Result<()> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        Self::verify_recovery(path)?;
        let record_path = files::sidecar(path, ".zero-recovery");
        let Some(raw) = files::read_optional(&record_path)? else {
            return Ok(());
        };
        let mut record: RecoveryRecord = serde_json::from_slice(&raw).map_err(io::Error::other)?;
        record.completed_phase = "committed".into();
        let permissions = std::fs::metadata(path)?.permissions();
        files::atomic_write(
            &record_path,
            &serde_json::to_vec(&record).map_err(io::Error::other)?,
            Some(permissions),
        )?;
        files::remove_sidecars(path)
    }

    /// Refuse further resource mutation while an earlier source result is
    /// Unknown. A committed metadata cleanup can be retried without replaying
    /// the already committed runtime operation.
    pub fn ensure_available(&self) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.disposition == SourceDisposition::Committed)
        {
            state.pending.as_mut().unwrap().finalize_committed(path)?;
            state.pending = None;
            state.observation.reason = None;
        }
        if let Some(pending) = &state.pending {
            return Err(SourcePersistError::new(
                pending.disposition,
                io::Error::other("configuration source has an unresolved original attempt"),
            ));
        }
        Ok(())
    }
    /// Only invoke after actual candidate Ready. Original bytes remain owned
    /// until final domain commit or confirmed source compensation.
    pub fn persist_ready(
        &self,
        config: &RuntimeConfig,
        identity: &str,
    ) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(pending) = &state.pending {
            return Err(SourcePersistError::new(
                pending.disposition,
                io::Error::other("another actual source attempt is still retained"),
            ));
        }
        let prepared = serde_json::to_vec_pretty(config)
            .map_err(io::Error::other)
            .and_then(|candidate| {
                PendingSource::prepare(path, &candidate, identity)
                    .map(|pending| (candidate, pending))
            });
        let result = match prepared {
            Ok((candidate, pending)) => {
                state.pending = Some(pending);
                state.pending.as_mut().unwrap().persist(path, &candidate)
            }
            Err(error) => Err(SourcePersistError::new(SourceDisposition::Unchanged, error)),
        };
        let disposition = state
            .pending
            .as_ref()
            .map_or(SourceDisposition::Unchanged, |pending| pending.disposition);
        state.observation.disposition = disposition;
        state.observation.observed_at_unix_ms = Some(now_ms());
        match &result {
            Ok(()) => {
                state.observation.successful_writes =
                    state.observation.successful_writes.wrapping_add(1);
                state.observation.writable = Some(true);
                state.observation.reason = None;
            }
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                state.observation.writable = Some(false);
                state.observation.reason = Some("source_write_permission_denied".into());
            }
            Err(_) => {
                state.observation.writable = None;
                state.observation.reason = Some("source_write_failed".into());
            }
        }
        result
    }
    pub fn rollback_pending(&self) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(pending) = state.pending.as_mut() else {
            return Ok(());
        };
        if pending.disposition == SourceDisposition::Committed {
            return Err(SourcePersistError::new(
                SourceDisposition::Committed,
                io::Error::other(
                    "published source commit cannot be rolled back as an uncommitted attempt",
                ),
            ));
        }
        let result = pending
            .restore_original(path)
            .map_err(|error| SourcePersistError::new(pending.disposition, error));
        state.observation.disposition = state.pending.as_ref().unwrap().disposition;
        result?;
        // Sidecar cleanup cannot erase the independently confirmed original
        // source outcome. Retained records still name that same original.
        let cleanup = files::remove_sidecars(path);
        state.pending = None;
        cleanup.map_err(|error| SourcePersistError::new(SourceDisposition::Restored, error))
    }
    pub fn finalize_committed(&self) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(pending) = state.pending.as_mut() else {
            return Ok(());
        };
        let result = pending.finalize_committed(path);
        state.observation.disposition = SourceDisposition::Committed;
        if result.is_ok() {
            state.pending = None;
        } else {
            state.observation.reason = Some("source_commit_metadata_pending".into());
        }
        result
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
