use super::*;
use crate::jsonl::Frame;
use std::io::BufReader;
use std::io::{self, Read};

impl DeliveryOutbox {
    pub(super) fn compact_if_needed(&mut self) -> ConnectorResult<()> {
        self.check_index()?;
        const MIN_RECORDS_BEFORE_COMPACTION: usize = 1024;
        if self.journal_records < MIN_RECORDS_BEFORE_COMPACTION
            || self.journal_records <= self.pending.len().saturating_mul(4).saturating_add(64)
        {
            return Ok(());
        }
        let status = match self.storage_status() {
            Err(error) => {
                tracing::warn!(
                    path = %self.path.display(),
                    %error,
                    "outbox compaction deferred because filesystem space could not be inspected"
                );
                return Ok(());
            }
            Ok(status) => status,
        };
        let maintenance_reserve = maintenance_reserve_bytes(status.reserve_bytes);
        let compaction_budget = status.available_bytes.saturating_sub(maintenance_reserve);
        if compaction_budget == 0 {
            warn_compaction_deferred(&self.path, status, 0);
            return Ok(());
        }

        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
            ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            }
        })?;
        let mut compacted = PendingIndex::new(&self.path)?;
        compacted.begin_batch()?;
        let mut deferred = false;
        {
            let mut reader = BufReader::new(open_reader(&self.path)?);
            self.pending.visit(|old_offset, key| {
                reader.seek(SeekFrom::Start(old_offset)).map_err(|source| {
                    ConnectorError::OpenOutbox {
                        path: self.path.display().to_string(),
                        source,
                    }
                })?;
                let new_offset = temporary
                    .as_file_mut()
                    .stream_position()
                    .map_err(|source| ConnectorError::OpenOutbox {
                        path: self.path.display().to_string(),
                        source,
                    })?;
                // The latest frame is already validated and durable. Copy it
                // verbatim instead of materializing potentially large payloads.
                let mut frame = Frame::new(&mut reader);
                let remaining = compaction_budget.saturating_sub(new_offset);
                let copied = io::copy(
                    &mut (&mut frame).take(remaining.saturating_add(1)),
                    temporary.as_file_mut(),
                )
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: self.path.display().to_string(),
                    source,
                })?;
                let projected_bytes = new_offset.saturating_add(copied);
                if projected_bytes > compaction_budget {
                    warn_compaction_deferred(&self.path, status, projected_bytes);
                    deferred = true;
                    return Ok(false);
                }
                if !frame.terminated {
                    return Err(ConnectorError::InvalidOutbox {
                        path: self.path.display().to_string(),
                        message: "pending journal frame is incomplete during compaction".into(),
                    });
                }
                compacted.insert(&key.0, &key.1, new_offset)?;
                Ok(true)
            })?;
        }
        if deferred {
            return Ok(());
        }
        compacted.finish_batch()?;
        temporary
            .as_file_mut()
            .sync_all()
            .map_err(|source| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            })?;
        self.file.take();
        if let Err(error) = temporary.persist(&self.path) {
            self.file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&self.path)
                .ok();
            return Err(ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source: error.error,
            });
        }
        // The rename committed a new journal layout. Until the file is open
        // and the derived index installed, no old offset may be read.
        self.index_valid = false;
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&self.path)
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: self.path.display().to_string(),
                    source,
                })?,
        );
        self.frame_cache.set(None);
        self.pending = compacted;
        self.index_valid = true;
        self.journal_records = self.pending.len();
        Ok(())
    }
}
