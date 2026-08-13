use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use zero_api::{OutboxStorageStatus, RawApiEvent};

use crate::registry::resolve_path;
use crate::state::PersistentStateLease;
use crate::{ConnectorError, ConnectorResult};

pub(crate) type DeliveryKey = (String, String);
type PendingDeliveries = BTreeMap<DeliveryKey, u64>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutboxInspection {
    pub(crate) bytes: u64,
    pub(crate) records: usize,
    pub(crate) pending: usize,
    pub(crate) recoverable_partial_tail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutboxDelivery {
    pub(crate) sink_tag: String,
    pub(crate) event: RawApiEvent,
    pub(crate) attempts: u32,
    pub(crate) message: Option<String>,
}

impl OutboxDelivery {
    pub(crate) fn key(&self) -> DeliveryKey {
        (self.sink_tag.clone(), self.event.event_id.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum JournalRecord {
    Put { delivery: Box<OutboxDelivery> },
    Ack { sink_tag: String, event_id: String },
}

pub(crate) struct DeliveryOutbox {
    path: std::path::PathBuf,
    _lease: PersistentStateLease,
    file: Option<File>,
    // Only durable keys and their latest journal offsets stay resident. Event
    // payloads are paged from disk so a long receiver outage cannot duplicate the
    // entire backlog in memory.
    pending: PendingDeliveries,
    pending_by_offset: BTreeMap<u64, DeliveryKey>,
    pending_counts: BTreeMap<String, usize>,
    journal_records: usize,
    min_free_bytes: u64,
    min_free_percent: u8,
}

impl DeliveryOutbox {
    pub(crate) fn open(
        path: &str,
        source_dir: Option<&Path>,
        min_free_bytes: u64,
        min_free_percent: u8,
    ) -> ConnectorResult<Self> {
        let path = resolve_path(path, source_dir);
        let lease = PersistentStateLease::acquire(&path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConnectorError::OpenOutbox {
                path: path.display().to_string(),
                source,
            })?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| ConnectorError::OpenOutbox {
                path: path.display().to_string(),
                source,
            })?;
        let (pending, journal_records) = read_pending(&path, &mut file)?;

        let (pending_by_offset, pending_counts) = build_pending_indexes(&pending);
        let mut outbox = Self {
            path,
            _lease: lease,
            file: Some(file),
            pending,
            pending_by_offset,
            pending_counts,
            journal_records,
            min_free_bytes,
            min_free_percent,
        };
        outbox.compact_if_needed()?;
        Ok(outbox)
    }

    pub(crate) fn load_pending_excluding(
        &self,
        excluded: &HashSet<DeliveryKey>,
        limit: usize,
    ) -> ConnectorResult<Vec<OutboxDelivery>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let selected = self
            .pending_by_offset
            .iter()
            .filter(|(_, key)| !excluded.contains(*key))
            .map(|(offset, _)| *offset);
        let mut reader = open_reader(&self.path)?;
        selected
            .take(limit)
            .map(|offset| read_delivery_at(&self.path, &mut reader, offset))
            .collect()
    }

    pub(crate) fn pending_counts(&self) -> BTreeMap<String, usize> {
        self.pending_counts.clone()
    }

    pub(crate) fn put(&mut self, delivery: &OutboxDelivery) -> ConnectorResult<()> {
        let record = JournalRecord::Put {
            delivery: Box::new(delivery.clone()),
        };
        let frame = self.serialize_record(&record)?;
        self.ensure_put_space(frame.len() as u64)?;
        let offset = self.append_frame(&frame)?;
        self.journal_records += 1;
        let key = delivery.key();
        if let Some(old_offset) = self.pending.insert(key.clone(), offset) {
            self.pending_by_offset.remove(&old_offset);
        } else {
            *self
                .pending_counts
                .entry(delivery.sink_tag.clone())
                .or_insert(0) += 1;
        }
        self.pending_by_offset.insert(offset, key);
        Ok(())
    }

    pub(crate) fn ack(&mut self, sink_tag: &str, event_id: &str) -> ConnectorResult<()> {
        // ACKs may use the normal PUT reserve so successfully delivered
        // entries can drain. They still retain an emergency maintenance floor
        // instead of being allowed to fill the filesystem without bound.
        let record = JournalRecord::Ack {
            sink_tag: sink_tag.to_owned(),
            event_id: event_id.to_owned(),
        };
        let frame = self.serialize_record(&record)?;
        self.ensure_maintenance_space(frame.len() as u64)?;
        self.append_frame(&frame)?;
        self.journal_records += 1;
        let key = (sink_tag.to_owned(), event_id.to_owned());
        if let Some(offset) = self.pending.remove(&key) {
            self.pending_by_offset.remove(&offset);
            if let Some(count) = self.pending_counts.get_mut(sink_tag) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.pending_counts.remove(sink_tag);
                }
            }
        }
        self.compact_if_needed()?;
        Ok(())
    }

    pub(crate) fn storage_status(&self) -> ConnectorResult<OutboxStorageStatus> {
        let probe_path = storage_probe_path(&self.path);
        let available_bytes =
            fs2::available_space(probe_path).map_err(|source| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            })?;
        let total_bytes =
            fs2::total_space(probe_path).map_err(|source| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            })?;
        let percent_reserve = ((total_bytes as u128 * self.min_free_percent as u128) / 100)
            .min(u64::MAX as u128) as u64;
        let reserve_bytes = self.min_free_bytes.max(percent_reserve);
        Ok(OutboxStorageStatus {
            available_bytes,
            total_bytes,
            reserve_bytes,
            maintenance_reserve_bytes: maintenance_reserve_bytes(reserve_bytes),
            write_blocked: available_bytes <= reserve_bytes,
        })
    }

    fn ensure_put_space(&self, attempted_write_bytes: u64) -> ConnectorResult<()> {
        let status = self.storage_status()?;
        self.ensure_space(status, status.reserve_bytes, attempted_write_bytes)
    }

    fn ensure_maintenance_space(&self, attempted_write_bytes: u64) -> ConnectorResult<()> {
        let status = self.storage_status()?;
        self.ensure_space(
            status,
            maintenance_reserve_bytes(status.reserve_bytes),
            attempted_write_bytes,
        )
    }

    fn ensure_space(
        &self,
        status: OutboxStorageStatus,
        reserve_bytes: u64,
        attempted_write_bytes: u64,
    ) -> ConnectorResult<()> {
        if status.available_bytes.saturating_sub(attempted_write_bytes) >= reserve_bytes {
            return Ok(());
        }
        Err(ConnectorError::OutboxStorageReserve {
            path: self.path.display().to_string(),
            available_bytes: status.available_bytes,
            reserve_bytes,
            attempted_write_bytes,
        })
    }

    fn serialize_record(&self, record: &JournalRecord) -> ConnectorResult<Vec<u8>> {
        let mut frame =
            serde_json::to_vec(record).map_err(|error| ConnectorError::InvalidOutbox {
                path: self.path.display().to_string(),
                message: format!("serialize journal record: {error}"),
            })?;
        frame.push(b'\n');
        Ok(frame)
    }

    fn append_frame(&mut self, frame: &[u8]) -> ConnectorResult<u64> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source: std::io::Error::other("outbox file is not open"),
            })?;
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(|source| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            })?;
        file.write_all(frame)
            .and_then(|_| file.flush())
            .and_then(|_| file.sync_data())
            .map_err(|source| ConnectorError::OpenOutbox {
                path: self.path.display().to_string(),
                source,
            })?;
        Ok(offset)
    }

    fn compact_if_needed(&mut self) -> ConnectorResult<()> {
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
        let mut compacted = BTreeMap::new();
        {
            let mut reader = open_reader(&self.path)?;
            for (key, old_offset) in &self.pending {
                let delivery = read_delivery_at(&self.path, &mut reader, *old_offset)?;
                let new_offset = temporary
                    .as_file_mut()
                    .stream_position()
                    .map_err(|source| ConnectorError::OpenOutbox {
                        path: self.path.display().to_string(),
                        source,
                    })?;
                let mut frame = serde_json::to_vec(&JournalRecord::Put {
                    delivery: Box::new(delivery),
                })
                .map_err(|error| ConnectorError::InvalidOutbox {
                    path: self.path.display().to_string(),
                    message: format!("serialize compacted journal record: {error}"),
                })?;
                frame.push(b'\n');
                let projected_bytes = new_offset.saturating_add(frame.len() as u64);
                if projected_bytes > compaction_budget {
                    warn_compaction_deferred(&self.path, status, projected_bytes);
                    return Ok(());
                }
                temporary
                    .write_all(&frame)
                    .map_err(|source| ConnectorError::OpenOutbox {
                        path: self.path.display().to_string(),
                        source,
                    })?;
                compacted.insert(key.clone(), new_offset);
            }
        }
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
        self.pending = compacted;
        (self.pending_by_offset, self.pending_counts) = build_pending_indexes(&self.pending);
        self.journal_records = self.pending.len();
        Ok(())
    }
}

fn build_pending_indexes(
    pending: &PendingDeliveries,
) -> (BTreeMap<u64, DeliveryKey>, BTreeMap<String, usize>) {
    let mut by_offset = BTreeMap::new();
    let mut counts = BTreeMap::new();
    for (key, offset) in pending {
        by_offset.insert(*offset, key.clone());
        *counts.entry(key.0.clone()).or_insert(0) += 1;
    }
    (by_offset, counts)
}

fn maintenance_reserve_bytes(reserve_bytes: u64) -> u64 {
    const MIN_MAINTENANCE_RESERVE_BYTES: u64 = 64 * 1024 * 1024;
    reserve_bytes
        .saturating_div(4)
        .max(MIN_MAINTENANCE_RESERVE_BYTES)
        .min(reserve_bytes)
}

fn storage_probe_path(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn warn_compaction_deferred(path: &Path, status: OutboxStorageStatus, temporary_bytes: u64) {
    tracing::warn!(
        path = %path.display(),
        available_bytes = status.available_bytes,
        reserve_bytes = status.reserve_bytes,
        temporary_bytes,
        "outbox compaction deferred to preserve filesystem free space"
    );
}

fn open_reader(path: &Path) -> ConnectorResult<File> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })
}

fn read_delivery_at(
    path: &Path,
    reader: &mut File,
    offset: u64,
) -> ConnectorResult<OutboxDelivery> {
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })?;
    let mut frame = Vec::new();
    BufReader::new(reader)
        .read_until(b'\n', &mut frame)
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })?;
    if frame.last() == Some(&b'\n') {
        frame.pop();
    }
    match serde_json::from_slice::<JournalRecord>(&frame).map_err(|error| {
        ConnectorError::InvalidOutbox {
            path: path.display().to_string(),
            message: format!("record at byte {offset}: {error}"),
        }
    })? {
        JournalRecord::Put { delivery } => Ok(*delivery),
        JournalRecord::Ack { .. } => Err(ConnectorError::InvalidOutbox {
            path: path.display().to_string(),
            message: format!("pending index at byte {offset} points to an ACK"),
        }),
    }
}

fn read_pending(path: &Path, file: &mut File) -> ConnectorResult<(PendingDeliveries, usize)> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })?;

    let has_partial_tail = bytes.last().is_some_and(|byte| *byte != b'\n');
    let valid_len = if has_partial_tail {
        bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or(0)
    } else {
        bytes.len()
    };
    if has_partial_tail {
        file.set_len(valid_len as u64)
            .map_err(|source| ConnectorError::OpenOutbox {
                path: path.display().to_string(),
                source,
            })?;
    }

    let (pending, journal_records) = parse_pending(path, &bytes[..valid_len])?;
    Ok((pending, journal_records))
}

pub(crate) fn inspect_path(path: &Path) -> ConnectorResult<OutboxInspection> {
    let bytes = std::fs::read(path).map_err(|source| ConnectorError::OpenOutbox {
        path: path.display().to_string(),
        source,
    })?;
    let recoverable_partial_tail = bytes.last().is_some_and(|byte| *byte != b'\n');
    let valid_len = if recoverable_partial_tail {
        bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map(|index| index + 1)
            .unwrap_or(0)
    } else {
        bytes.len()
    };
    let (pending, records) = parse_pending(path, &bytes[..valid_len])?;
    Ok(OutboxInspection {
        bytes: bytes.len() as u64,
        records,
        pending: pending.len(),
        recoverable_partial_tail,
    })
}

fn parse_pending(path: &Path, bytes: &[u8]) -> ConnectorResult<(PendingDeliveries, usize)> {
    let mut pending = BTreeMap::new();
    let mut journal_records = 0;
    let mut start = 0usize;
    while start < bytes.len() {
        let relative_end = bytes[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .expect("valid outbox frames end with a newline");
        let end = start + relative_end;
        if end > start {
            let record =
                serde_json::from_slice::<JournalRecord>(&bytes[start..end]).map_err(|error| {
                    ConnectorError::InvalidOutbox {
                        path: path.display().to_string(),
                        message: format!("record {}: {error}", journal_records + 1),
                    }
                })?;
            journal_records += 1;
            match record {
                JournalRecord::Put { delivery } => {
                    pending.insert(delivery.key(), start as u64);
                }
                JournalRecord::Ack { sink_tag, event_id } => {
                    pending.remove(&(sink_tag, event_id));
                }
            }
        }
        start = end + 1;
    }
    Ok((pending, journal_records))
}
