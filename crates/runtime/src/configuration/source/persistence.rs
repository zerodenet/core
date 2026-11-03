//! The existing source transaction also supplies the embedded commit path.
use super::*;

impl ConfigurationSource {
    fn ensure_commit_io_available(&self) -> Result<(), SourcePersistError> {
        super::super::startup::confirm_closed()
            .map_err(|error| SourcePersistError::new(SourceDisposition::Unknown, error))?;
        self.ensure_resource_io_available()
            .map_err(|error| SourcePersistError::new(SourceDisposition::Unknown, error))
    }
    pub fn ensure_available(&self) -> Result<(), SourcePersistError> {
        self.ensure_commit_io_available()?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.ensure_state_available(&mut state)
    }
    pub(super) fn ensure_state_available(
        &self,
        state: &mut SourceState,
    ) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        if state
            .pending
            .as_ref()
            .is_some_and(|pending| pending.disposition == SourceDisposition::Committed)
        {
            state
                .pending
                .as_mut()
                .unwrap()
                .finalize_committed(path, &self.io)?;
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
        self.ensure_commit_io_available()?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.persist_ready_locked(config, identity, &mut state)
    }
    pub(super) fn persist_ready_locked(
        &self,
        config: &RuntimeConfig,
        identity: &str,
        state: &mut SourceState,
    ) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        if let Some(pending) = &state.pending {
            return Err(SourcePersistError::new(
                pending.disposition,
                io::Error::other("another actual source attempt is still retained"),
            ));
        }
        let prepared = serde_json::to_vec_pretty(config)
            .map_err(io::Error::other)
            .and_then(|candidate| {
                PendingSource::prepare(path, &candidate, identity, &self.io)
                    .map(|pending| (candidate, pending))
            });
        let result = match prepared {
            Ok((candidate, pending)) => {
                state.pending = Some(pending);
                state
                    .pending
                    .as_mut()
                    .unwrap()
                    .persist(path, &candidate, &self.io)
            }
            Err(error) => Err(SourcePersistError::new(
                if self.io.ensure_available().is_err() {
                    SourceDisposition::Unknown
                } else {
                    SourceDisposition::Unchanged
                },
                error,
            )),
        };
        let disposition = if self.io.ensure_available().is_err() {
            SourceDisposition::Unknown
        } else {
            state
                .pending
                .as_ref()
                .map_or(SourceDisposition::Unchanged, |pending| pending.disposition)
        };
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
        self.ensure_commit_io_available()?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.rollback_pending_locked(&mut state)
    }
    pub(super) fn rollback_pending_locked(
        &self,
        state: &mut SourceState,
    ) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
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
            .restore_original(path, &self.io)
            .map_err(|error| SourcePersistError::new(pending.disposition, error));
        state.observation.disposition = state.pending.as_ref().unwrap().disposition;
        result?;
        let cleanup = files::remove_sidecars(path, &self.io);
        state.pending = None;
        let disposition = if self.io.ensure_available().is_err() {
            SourceDisposition::Unknown
        } else {
            SourceDisposition::Restored
        };
        state.observation.disposition = disposition;
        cleanup.map_err(|error| SourcePersistError::new(disposition, error))
    }
    pub fn finalize_committed(&self) -> Result<(), SourcePersistError> {
        self.ensure_commit_io_available()?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        self.finalize_committed_locked(&mut state)
    }
    pub(super) fn finalize_committed_locked(
        &self,
        state: &mut SourceState,
    ) -> Result<(), SourcePersistError> {
        let Some(path) = self.path() else {
            return Ok(());
        };
        let Some(pending) = state.pending.as_mut() else {
            return Ok(());
        };
        let result = pending.finalize_committed(path, &self.io);
        state.observation.disposition = if self.io.ensure_available().is_err() {
            SourceDisposition::Unknown
        } else {
            SourceDisposition::Committed
        };
        if result.is_ok() {
            state.pending = None;
        } else {
            state.observation.reason = Some("source_commit_metadata_pending".into());
        }
        result
    }
}
