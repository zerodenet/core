use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use zero_api::{ApiEvent, OutboxRecoveryState, OutboxRecoveryStatus, OutboxStorageStatus};

use crate::registry::resolve_path;
use crate::state::PersistentStateLease;
use crate::{ConnectorError, ConnectorResult};

mod compaction;
mod paging;
mod record;
mod recovery;
mod replay;
#[cfg(test)]
mod tests;

use recovery::{corruption_class, quarantine_journal};
pub(crate) use replay::inspect_path;
use replay::read_pending;

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
pub(crate) struct OutboxDelivery<P = serde_json::Value> {
    pub(crate) sink_tag: String,
    pub(crate) event: ApiEvent<P>,
    pub(crate) attempts: u32,
    pub(crate) message: Option<String>,
}

pub(crate) struct PendingPage {
    pub(crate) deliveries: Vec<(OutboxDelivery, u64)>,
    pub(crate) waiting_for_bytes: bool,
}

impl<P> OutboxDelivery<P> {
    pub(crate) fn key(&self) -> DeliveryKey {
        (self.sink_tag.clone(), self.event.event_id.clone())
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum JournalRecord<P = serde_json::Value> {
    Put { delivery: Box<OutboxDelivery<P>> },
    Ack { sink_tag: String, event_id: String },
}

pub(crate) struct DeliveryOutbox {
    path: std::path::PathBuf,
    // Only the last candidate frame size is cached; never another backlog index.
    frame_cache: std::cell::Cell<Option<(u64, u64)>>,
    #[cfg(test)]
    frame_scans: std::cell::Cell<usize>,
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
    recovery: Option<OutboxRecoveryStatus>,
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

        let mut file = open_journal(&path, false)?;
        let (pending, journal_records, recovery) = match read_pending(&path, &mut file) {
            Ok(state) => state,
            Err(ConnectorError::InvalidOutbox { message, .. }) => {
                drop(file);
                let preserved_path = quarantine_journal(&path)?;
                tracing::warn!(
                    path = %path.display(),
                    preserved_path = %preserved_path.display(),
                    corruption_class = ?corruption_class(&message),
                    error = %message,
                    "corrupt delivery outbox quarantined; connector started with a new journal and requires reconciliation"
                );
                file = open_journal(&path, true)?;
                (
                    BTreeMap::new(),
                    0,
                    Some(OutboxRecoveryStatus {
                        state: OutboxRecoveryState::RecoveryRequired,
                        corruption_class: corruption_class(&message),
                        preserved_path: preserved_path.display().to_string(),
                        message,
                    }),
                )
            }
            Err(error) => return Err(error),
        };

        let (pending_by_offset, pending_counts) = build_pending_indexes(&pending);
        let mut outbox = Self {
            path,
            frame_cache: std::cell::Cell::new(None),
            #[cfg(test)]
            frame_scans: std::cell::Cell::new(0),
            _lease: lease,
            file: Some(file),
            pending,
            pending_by_offset,
            pending_counts,
            journal_records,
            min_free_bytes,
            min_free_percent,
            recovery,
        };
        outbox.compact_if_needed()?;
        Ok(outbox)
    }

    pub(crate) fn pending_counts(&self) -> BTreeMap<String, usize> {
        self.pending_counts.clone()
    }

    pub(crate) fn recovery_status(&self) -> Option<OutboxRecoveryStatus> {
        self.recovery.clone()
    }

    #[cfg(test)]
    pub(crate) fn put(&mut self, delivery: &OutboxDelivery) -> ConnectorResult<()> {
        self.put_ref(
            &delivery.sink_tag,
            &delivery.event,
            delivery.attempts,
            delivery.message.as_deref(),
        )
    }

    pub(crate) fn put_ref(
        &mut self,
        sink_tag: &str,
        event: &zero_api::RawApiEvent,
        attempts: u32,
        message: Option<&str>,
    ) -> ConnectorResult<()> {
        #[derive(Serialize)]
        struct BorrowedDelivery<'a> {
            sink_tag: &'a str,
            event: &'a zero_api::RawApiEvent,
            attempts: u32,
            message: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct Put<'a> {
            op: &'static str,
            delivery: BorrowedDelivery<'a>,
        }
        let record = Put {
            op: "put",
            delivery: BorrowedDelivery {
                sink_tag,
                event,
                attempts,
                message,
            },
        };
        let frame = self.serialize_record(&record)?;
        self.ensure_put_space(frame.len() as u64)?;
        let offset = self.append_frame(&frame)?;
        self.journal_records += 1;
        let key = (sink_tag.to_owned(), event.event_id.clone());
        if let Some(old_offset) = self.pending.insert(key.clone(), offset) {
            self.pending_by_offset.remove(&old_offset);
        } else {
            *self.pending_counts.entry(sink_tag.to_owned()).or_insert(0) += 1;
        }
        self.pending_by_offset.insert(offset, key);
        // Receiver outages still append PUTs when attempts/messages change.
        // Superseded PUTs must not wait for a successful ACK or a restart.
        self.compact_if_needed()?;
        Ok(())
    }

    pub(crate) fn ack(&mut self, sink_tag: &str, event_id: &str) -> ConnectorResult<()> {
        // ACKs may use the normal PUT reserve so successfully delivered
        // entries can drain. They still retain an emergency maintenance floor
        // instead of being allowed to fill the filesystem without bound.
        let record: JournalRecord = JournalRecord::Ack {
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

    fn serialize_record<T: Serialize>(&self, record: &T) -> ConnectorResult<Vec<u8>> {
        let mut frame =
            crate::encoding::to_vec(record).map_err(|error| ConnectorError::InvalidOutbox {
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
    // Maintenance records are tiny and must be able to drain acknowledged
    // deliveries even when a percentage-based PUT reserve expands to many
    // gigabytes. Preserve at most 64 MiB for ACK/compaction bookkeeping while
    // never exceeding the operator's configured reserve.
    const MAX_MAINTENANCE_RESERVE_BYTES: u64 = 64 * 1024 * 1024;
    reserve_bytes.min(MAX_MAINTENANCE_RESERVE_BYTES)
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

fn open_journal(path: &Path, truncate: bool) -> ConnectorResult<File> {
    OpenOptions::new()
        .create(true)
        .truncate(truncate)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })
}
