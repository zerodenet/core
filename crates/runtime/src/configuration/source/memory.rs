//! Concrete Source byte backing; no parser, IO grant, task or second book.
use super::super::{ControlBudget, ControlBudgetCandidate, ControlBudgetPermit};
use super::*;
use std::{
    io::{Read, Write},
    sync::{Arc, Weak},
};
pub(super) struct Bytes {
    bytes: Vec<u8>,
    _credit: Option<ControlBudgetPermit>,
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
pub(super) struct Memory<'a> {
    pub book: Option<&'a Arc<ControlBudget>>,
    pub candidate: Option<&'a ControlBudgetCandidate>,
    pub previous_max: usize,
    pub output_max: usize,
    pub total_max: usize,
}
impl Memory<'_> {
    pub(super) fn ticket(
        &self,
        length: usize,
        other: usize,
    ) -> io::Result<Option<ControlBudgetPermit>> {
        if length
            .checked_add(other)
            .is_none_or(|sum| sum > self.total_max)
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "Source backing capacity exhausted",
            ));
        }
        self.book
            .map(|book| book.reserve_source_bytes(length, self.candidate))
            .transpose()
    }
    fn allocate(&self, length: usize, other: usize) -> io::Result<Bytes> {
        // Permit declared first: every error also destroys data before refund.
        let credit = self.ticket(length, other)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        Ok(Bytes {
            bytes,
            _credit: credit,
        })
    }
    pub fn pretty(&self, value: &RuntimeConfig) -> io::Result<Bytes> {
        let mut count = Count(0);
        serde_json::to_writer_pretty(&mut count, value).map_err(io::Error::other)?;
        if count.0 > self.output_max {
            return Err(too_large());
        }
        let mut output = self.allocate(count.0, 0)?;
        // Fixed-capacity writer rejects serializer drift before Vec can grow.
        let mut writer = Fixed {
            bytes: &mut output.bytes,
            length: count.0,
        };
        serde_json::to_writer_pretty(&mut writer, value).map_err(io::Error::other)?;
        Ok(output)
    }
    pub fn previous(
        &self,
        path: &Path,
        owner: &super::super::io::ConfigurationIo,
        other: usize,
    ) -> io::Result<Option<Bytes>> {
        let mut output = None; // lives outside original-file registry guard
        let result = owner.with_file(
            || std::fs::File::open(path),
            |file| {
                let length = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
                if length > self.previous_max {
                    return Err(too_large());
                }
                output = Some(self.allocate(length, other)?);
                let bytes = &mut output.as_mut().unwrap().bytes;
                bytes.resize(length, 0);
                file.read_exact(bytes)?;
                let mut extra = [0];
                if file.read(&mut extra)? != 0 {
                    return Err(too_large());
                }
                Ok(())
            },
        );
        match result {
            Ok(()) => Ok(output),
            Err(error)
                if error.kind() == io::ErrorKind::NotFound && owner.ensure_available().is_ok() =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
// The fixed RecoveryRecord has two identity Strings, two 64-byte hex digest
// Strings, one phase String and an attempt PID/timestamp suffix. Four times
// identity length covers the actual format! growth plus identity.to_owned;
// 512 covers digest formatting growth, suffix/phase overlap and alignment.
// This is a conservative schema-specific heap promise, not process RSS.
pub(super) fn record_storage(identity: &str) -> io::Result<usize> {
    identity
        .len()
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or_else(too_large)
}
pub(super) struct Count(pub usize);
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(too_large)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Fixed<'a> {
    bytes: &'a mut Vec<u8>,
    length: usize,
}
impl Write for Fixed<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.length.saturating_sub(self.bytes.len()) {
            return Err(too_large());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn too_large() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Source bytes exceed the authoritative file limit",
    )
}
impl ConfigurationSource {
    /// Production binds the book already created by this actual Host. There is
    /// no Source-created ControlBudget and no Kernel fallback to embedded mode.
    pub fn bind_persistence_budget(&self, book: &Arc<ControlBudget>) -> io::Result<()> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.pending.is_some() {
            return Err(io::Error::other("Source attempt remains retained"));
        }
        let weak = Arc::downgrade(book);
        let stored = self.persistence_book.get_or_init(|| weak.clone());
        if !Weak::ptr_eq(stored, &weak) {
            return Err(io::Error::other(
                "Source belongs to a different control book",
            ));
        }
        Ok(())
    }
    pub(super) fn embedded_memory<'a>(
        &self,
        previous: &zero_config::RuntimeOptionsConfig,
        target: &zero_config::RuntimeOptionsConfig,
    ) -> io::Result<Memory<'a>> {
        if self.persistence_book.get().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Kernel Source requires its actual Host budget",
            ));
        }
        Ok(Memory {
            book: None,
            candidate: None,
            previous_max: previous.effective_file_read_max_bytes(),
            output_max: target.effective_file_read_max_bytes(),
            total_max: target.effective_control_max_in_flight_response_bytes(),
        })
    }
    pub fn persist_ready_with_budget(
        &self,
        config: &RuntimeConfig,
        identity: &str,
        book: &Arc<ControlBudget>,
        candidate: Option<&ControlBudgetCandidate>,
        previous_max: usize,
    ) -> Result<(), SourcePersistError> {
        if !self
            .persistence_book
            .get()
            .is_some_and(|stored| Weak::ptr_eq(stored, &Arc::downgrade(book)))
        {
            return Err(SourcePersistError::new(
                SourceDisposition::Unchanged,
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Source has no matching Host budget",
                ),
            ));
        }
        let _notifications = book.defer_source_notifications();
        self.ensure_commit_io_available()?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let memory = Memory {
            book: Some(book),
            candidate,
            previous_max,
            output_max: config.runtime.effective_file_read_max_bytes(),
            total_max: candidate.map_or(book.limits().messages.response_bytes_total, |candidate| {
                candidate.limits().messages.response_bytes_total
            }),
        };
        self.persist_ready_locked(config, identity, &mut state, &memory)
    }
}

/// Scoped wake deferral only. Caller holds it outside its real Host/catalog
/// publication gate; it grants no allocation, publication, IO or close right.
pub struct SourcePersistenceNotifications {
    _notifications: Option<super::super::control_budget::SourceNotifications>,
}
impl ConfigurationSource {
    pub fn defer_persistence_notifications(&self) -> SourcePersistenceNotifications {
        SourcePersistenceNotifications {
            _notifications: self
                .persistence_book
                .get()
                .and_then(Weak::upgrade)
                .map(|book| book.defer_source_notifications()),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/source_memory.rs"]
mod tests;
