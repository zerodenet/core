use super::{files::*, SourceDisposition, SourcePersistError};
use serde::{Deserialize, Serialize};
use std::{io, path::Path};

#[derive(Serialize, Deserialize)]
pub(super) struct RecoveryRecord {
    pub schema: u8,
    pub attempt: String,
    pub old_digest: Option<String>,
    pub candidate_digest: String,
    pub persistent: bool,
    pub completed_phase: String,
    pub compensation_identity: String,
}
pub(super) struct PendingSource {
    pub record: RecoveryRecord,
    previous: Option<Vec<u8>>,
    permissions: Option<std::fs::Permissions>,
    pub disposition: SourceDisposition,
}
impl PendingSource {
    pub fn prepare(path: &Path, candidate: &[u8], identity: &str) -> io::Result<Self> {
        let previous = read_optional(path)?;
        let permissions = std::fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions());
        Ok(Self {
            record: RecoveryRecord {
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
            },
            previous,
            permissions,
            disposition: SourceDisposition::Unchanged,
        })
    }
    fn record(&self, path: &Path) -> io::Result<()> {
        atomic_write(
            &sidecar(path, ".zero-recovery"),
            &serde_json::to_vec(&self.record).map_err(io::Error::other)?,
            self.permissions.clone(),
        )
    }
    fn failure(&self, error: io::Error) -> SourcePersistError {
        SourcePersistError::new(self.disposition, error)
    }
    pub fn persist(&mut self, path: &Path, candidate: &[u8]) -> Result<(), SourcePersistError> {
        if let Some(previous) = &self.previous {
            atomic_write(
                &sidecar(path, ".zero-last-good"),
                previous,
                self.permissions.clone(),
            )
            .map_err(|error| self.failure(error))?;
        }
        self.record(path).map_err(|error| self.failure(error))?;
        // An atomic rename can succeed before directory sync fails. Retain the
        // original bytes and an Unknown outcome before attempting either.
        self.disposition = SourceDisposition::Unknown;
        if let Err(error) = atomic_write(path, candidate, self.permissions.clone()) {
            return match self.restore_original(path) {
                Ok(()) => Err(self.failure(error)),
                Err(restore) => Err(self.failure(
                    super::model::SourceRecoveryFailure {
                        stage: "source replacement",
                        original: error,
                        recovery: restore,
                    }
                    .into_io(),
                )),
            };
        }
        self.disposition = SourceDisposition::Persisted;
        self.record.completed_phase = "persisted".into();
        if let Err(error) = self.record(path) {
            return match self.restore_original(path) {
                Ok(()) => Err(self.failure(error)),
                Err(restore) => Err(self.failure(
                    super::model::SourceRecoveryFailure {
                        stage: "source record",
                        original: error,
                        recovery: restore,
                    }
                    .into_io(),
                )),
            };
        }
        Ok(())
    }
    /// Consume actual file outcomes, including original absence. Failure keeps
    /// this same pending owner and forbids constructing recovery resources.
    pub fn restore_original(&mut self, path: &Path) -> io::Result<()> {
        self.disposition = SourceDisposition::Unknown;
        let actual = read_optional(path)?;
        let actual_digest = actual.as_ref().map(|bytes| digest(bytes));
        if actual_digest != self.record.old_digest {
            if actual_digest.as_ref() != Some(&self.record.candidate_digest) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "actual source matches neither original nor candidate",
                ));
            }
            match &self.previous {
                Some(previous) => atomic_write(path, previous, self.permissions.clone())?,
                None => {
                    std::fs::remove_file(path)?;
                    sync_parent(path)?;
                }
            }
        } else {
            sync_parent(path)?;
        }
        let restored = read_optional(path)?.as_ref().map(|bytes| digest(bytes));
        if restored != self.record.old_digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "original source restoration remains unconfirmed",
            ));
        }
        self.disposition = SourceDisposition::Restored;
        self.record.completed_phase = "restored".into();
        Ok(())
    }
    /// The domain commit already happened. A metadata failure is a committed
    /// operation error, never permission to roll back its published revision.
    pub fn finalize_committed(&mut self, path: &Path) -> Result<(), SourcePersistError> {
        self.disposition = SourceDisposition::Committed;
        self.record.completed_phase = "committed".into();
        self.record(path).map_err(|error| self.failure(error))?;
        remove_sidecars(path).map_err(|error| self.failure(error))
    }
}
