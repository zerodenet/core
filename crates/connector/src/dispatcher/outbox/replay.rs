use std::fs::File;
use std::io::{self, BufReader, Seek};
use std::path::Path;

use serde::de::IgnoredAny;
use zero_api::{OutboxRecoveryState, OutboxRecoveryStatus};

use super::recovery::preserve_journal;
use super::{JournalRecord, OutboxInspection, PendingDeliveries};
use crate::jsonl::{has_complete_frame, read_record, Snapshot, BUFFER_BYTES};
use crate::{ConnectorError, ConnectorResult};

struct Replay {
    pending: PendingDeliveries,
    records: usize,
    bytes: u64,
    valid_bytes: u64,
}

fn io_error(path: &Path, source: io::Error) -> ConnectorError {
    ConnectorError::OpenOutbox {
        path: path.display().to_string(),
        source,
    }
}

fn scan(path: &Path, file: &mut File) -> ConnectorResult<Replay> {
    let bytes = file
        .metadata()
        .map_err(|error| io_error(path, error))?
        .len();
    file.rewind().map_err(|error| io_error(path, error))?;
    // Bound the view to the open inode's initial length. A live writer can
    // append or atomically compact without making inspection chase its tail.
    let mut reader = BufReader::with_capacity(BUFFER_BYTES, Snapshot::new(file, bytes));
    let mut pending = PendingDeliveries::new();
    let mut records = 0;
    let mut offset = 0;
    let mut valid_bytes = 0;
    let mut prefix = Vec::with_capacity(BUFFER_BYTES);
    while offset < bytes {
        let Some(frame) = read_record::<JournalRecord<IgnoredAny>>(&mut reader, &mut prefix)
            .map_err(|error| io_error(path, error))?
        else {
            break;
        };
        let next_offset = offset + frame.bytes;
        if !frame.terminated {
            break;
        }
        valid_bytes = next_offset;
        if let Some(parsed) = frame.parsed {
            let record = match parsed {
                Ok(record) => record,
                Err(error) => {
                    let class = if has_complete_frame(&mut reader)
                        .map_err(|error| io_error(path, error))?
                    {
                        "malformed_middle"
                    } else {
                        "malformed_tail"
                    };
                    return Err(ConnectorError::InvalidOutbox {
                        path: path.display().to_string(),
                        message: format!(
                            "{class} at record {} byte {offset}: {error}",
                            records + 1
                        ),
                    });
                }
            };
            records += 1;
            match record {
                JournalRecord::Put { delivery } => {
                    pending.insert(delivery.key(), offset);
                }
                JournalRecord::Ack { sink_tag, event_id } => {
                    pending.remove(&(sink_tag, event_id));
                }
            }
        }
        offset = next_offset;
    }
    Ok(Replay {
        pending,
        records,
        bytes,
        valid_bytes,
    })
}

pub(super) fn read_pending(
    path: &Path,
    file: &mut File,
) -> ConnectorResult<(PendingDeliveries, usize, Option<OutboxRecoveryStatus>)> {
    let replay = scan(path, file)?;
    let recovery = if replay.valid_bytes < replay.bytes {
        let preserved_path = preserve_journal(path, file, replay.bytes)?;
        file.set_len(replay.valid_bytes)
            .and_then(|_| file.sync_data())
            .map_err(|error| io_error(path, error))?;
        let message = format!(
            "truncated {} partial tail bytes at byte {}",
            replay.bytes - replay.valid_bytes,
            replay.valid_bytes
        );
        tracing::warn!(path = %path.display(), preserved_path = %preserved_path.display(),
            rejected_bytes = replay.bytes - replay.valid_bytes,
            "recovered partial delivery outbox tail and preserved the rejected bytes");
        Some(OutboxRecoveryStatus {
            state: OutboxRecoveryState::Recovered,
            corruption_class: zero_api::OutboxCorruptionClass::PartialTail,
            preserved_path: preserved_path.display().to_string(),
            message,
        })
    } else {
        None
    };
    Ok((replay.pending, replay.records, recovery))
}

pub(crate) fn inspect_path(path: &Path) -> ConnectorResult<OutboxInspection> {
    let mut file = File::open(path).map_err(|error| io_error(path, error))?;
    let replay = scan(path, &mut file)?;
    Ok(OutboxInspection {
        bytes: replay.bytes,
        records: replay.records,
        pending: replay.pending.len(),
        recoverable_partial_tail: replay.valid_bytes < replay.bytes,
    })
}
