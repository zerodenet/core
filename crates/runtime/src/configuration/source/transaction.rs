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
    previous: Option<super::memory::Bytes>,
    file_max: usize,
    permissions: Option<std::fs::Permissions>,
    pub disposition: SourceDisposition,
    // Record Strings and previous backing must die before their own credits.
    _record_credit: Option<super::super::ControlBudgetPermit>,
}
impl PendingSource {
    pub fn prepare(
        path: &Path,
        candidate: &[u8],
        identity: &str,
        owner: &super::super::io::ConfigurationIo,
        memory: &super::memory::Memory<'_>,
    ) -> io::Result<Self> {
        let previous = memory.previous(path, owner, candidate.len())?;
        let permissions = std::fs::metadata(path)
            .ok()
            .map(|metadata| metadata.permissions());
        let file_max = memory.previous_max.max(memory.output_max);
        if identity.len() > memory.previous_max.min(memory.output_max) / 2 {
            return Err(super::memory::too_large());
        }
        let held = candidate
            .len()
            .checked_add(previous.as_ref().map_or(0, |bytes| bytes.len()))
            .ok_or_else(super::memory::too_large)?;
        let record_credit = memory.ticket(super::memory::record_storage(identity)?, held)?;
        let pending = Self {
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
            file_max,
            permissions,
            disposition: SourceDisposition::Unchanged,
            _record_credit: record_credit,
        };
        let mut encoded = super::memory::Count(0);
        serde_json::to_writer(&mut encoded, &pending.record).map_err(io::Error::other)?;
        // "ready" may later become the nine-byte "committed"/"persisted".
        if encoded
            .0
            .checked_add(4)
            .is_none_or(|size| size > memory.previous_max.min(memory.output_max))
        {
            return Err(super::memory::too_large());
        }
        Ok(pending)
    }
    fn record(&self, path: &Path, owner: &super::super::io::ConfigurationIo) -> io::Result<()> {
        atomic_write_record(
            &sidecar(path, ".zero-recovery"),
            &self.record,
            self.permissions.clone(),
            owner,
        )
    }
    fn failure(
        &self,
        error: io::Error,
        owner: &super::super::io::ConfigurationIo,
    ) -> SourcePersistError {
        SourcePersistError::new(
            if owner.ensure_available().is_err() {
                SourceDisposition::Unknown
            } else {
                self.disposition
            },
            error,
        )
    }
    pub fn persist(
        &mut self,
        path: &Path,
        candidate: &[u8],
        owner: &super::super::io::ConfigurationIo,
    ) -> Result<(), SourcePersistError> {
        if let Some(previous) = &self.previous {
            atomic_write(
                &sidecar(path, ".zero-last-good"),
                previous,
                self.permissions.clone(),
                owner,
            )
            .map_err(|error| self.failure(error, owner))?;
        }
        self.record(path, owner)
            .map_err(|error| self.failure(error, owner))?;
        // An atomic rename can succeed before directory sync fails. Retain the
        // original bytes and an Unknown outcome before attempting either.
        self.disposition = SourceDisposition::Unknown;
        if let Err(error) = atomic_write(path, candidate, self.permissions.clone(), owner) {
            return match self.restore_original(path, owner) {
                Ok(()) => Err(self.failure(error, owner)),
                Err(restore) => Err(self.failure(
                    super::model::SourceRecoveryFailure {
                        stage: "source replacement",
                        original: error,
                        recovery: restore,
                    }
                    .into_io(),
                    owner,
                )),
            };
        }
        self.disposition = SourceDisposition::Persisted;
        self.record.completed_phase = "persisted".into();
        if let Err(error) = self.record(path, owner) {
            return match self.restore_original(path, owner) {
                Ok(()) => Err(self.failure(error, owner)),
                Err(restore) => Err(self.failure(
                    super::model::SourceRecoveryFailure {
                        stage: "source record",
                        original: error,
                        recovery: restore,
                    }
                    .into_io(),
                    owner,
                )),
            };
        }
        Ok(())
    }
    /// Consume actual file outcomes, including original absence. Failure keeps
    /// this same pending owner and forbids constructing recovery resources.
    pub fn restore_original(
        &mut self,
        path: &Path,
        owner: &super::super::io::ConfigurationIo,
    ) -> io::Result<()> {
        self.disposition = SourceDisposition::Unknown;
        owner.ensure_available()?;
        let actual_digest = digest_optional(path, owner, self.file_max)?;
        if actual_digest != self.record.old_digest {
            if actual_digest.as_ref() != Some(&self.record.candidate_digest) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "actual source matches neither original nor candidate",
                ));
            }
            match &self.previous {
                Some(previous) => atomic_write(path, previous, self.permissions.clone(), owner)?,
                None => {
                    std::fs::remove_file(path)?;
                    sync_parent(path, owner)?;
                }
            }
        } else {
            sync_parent(path, owner)?;
        }
        let restored = digest_optional(path, owner, self.file_max)?;
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
    pub fn finalize_committed(
        &mut self,
        path: &Path,
        owner: &super::super::io::ConfigurationIo,
    ) -> Result<(), SourcePersistError> {
        self.disposition = SourceDisposition::Committed;
        self.record.completed_phase = "committed".into();
        self.record(path, owner)
            .map_err(|error| self.failure(error, owner))?;
        remove_sidecars(path, owner).map_err(|error| self.failure(error, owner))
    }
}
