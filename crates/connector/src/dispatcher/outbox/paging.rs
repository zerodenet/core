//! Select a byte-bounded disk working set before allocating event payloads.
use super::*;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

impl DeliveryOutbox {
    #[cfg(test)]
    pub(crate) fn load_pending_excluding(
        &self,
        excluded: &HashSet<DeliveryKey>,
        limit: usize,
    ) -> ConnectorResult<Vec<OutboxDelivery>> {
        Ok(self
            .load_pending_with_budget(excluded, limit, u64::MAX, true)?
            .into_iter()
            .map(|(delivery, _)| delivery)
            .collect())
    }

    #[cfg(test)]
    pub(crate) fn load_pending_with_budget(
        &self,
        excluded: &HashSet<DeliveryKey>,
        limit: usize,
        bytes: u64,
        allow_oversized: bool,
    ) -> ConnectorResult<Vec<(OutboxDelivery, u64)>> {
        Ok(self
            .load_pending_page(excluded, limit, bytes, allow_oversized)?
            .deliveries)
    }

    pub(crate) fn load_pending_page(
        &self,
        excluded: &HashSet<DeliveryKey>,
        limit: usize,
        mut bytes: u64,
        allow_oversized: bool,
    ) -> ConnectorResult<PendingPage> {
        self.check_index()?;
        if limit == 0 || (bytes == 0 && !allow_oversized) {
            return Ok(PendingPage {
                deliveries: Vec::new(),
                waiting_for_bytes: false,
            });
        }
        let mut reader = BufReader::new(open_reader(&self.path)?);
        let mut deliveries = Vec::new();
        let mut waiting_for_bytes = false;
        self.pending.visit(|offset, key| {
            if excluded.contains(&key) {
                return Ok(true);
            }
            let size = match self.frame_cache.get() {
                Some((cached, size)) if cached == offset => size,
                _ => {
                    #[cfg(test)]
                    self.frame_scans.set(self.frame_scans.get() + 1);
                    let size = frame_length(&self.path, &mut reader, offset)?;
                    self.frame_cache.set(Some((offset, size)));
                    size
                }
            };
            if size > bytes && !(allow_oversized && deliveries.is_empty()) {
                waiting_for_bytes = true;
                return Ok(false);
            }
            let delivery = read_delivery_at(&self.path, &mut reader, offset, size)?;
            deliveries.push((delivery, size));
            bytes = bytes.saturating_sub(size);
            if deliveries.len() == limit || bytes == 0 {
                return Ok(false);
            }
            Ok(true)
        })?;
        Ok(PendingPage {
            deliveries,
            waiting_for_bytes,
        })
    }
}

fn frame_length(path: &Path, reader: &mut BufReader<File>, offset: u64) -> ConnectorResult<u64> {
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|e| read_error(path, e))?;
    let mut size = 0u64;
    loop {
        let buffer = reader.fill_buf().map_err(|e| read_error(path, e))?;
        if buffer.is_empty() {
            return Err(ConnectorError::InvalidOutbox {
                path: path.display().to_string(),
                message: format!("incomplete pending frame at byte {offset}"),
            });
        }
        let end = buffer.iter().position(|b| *b == b'\n');
        let consumed = end.map_or(buffer.len(), |i| i + 1);
        size += consumed as u64;
        reader.consume(consumed);
        if end.is_some() {
            return Ok(size);
        }
    }
}

fn read_delivery_at(
    path: &Path,
    reader: &mut BufReader<File>,
    offset: u64,
    size: u64,
) -> ConnectorResult<OutboxDelivery> {
    reader
        .seek(SeekFrom::Start(offset))
        .map_err(|e| read_error(path, e))?;
    // Only the parsed event is retained. Do not allocate a second full frame.
    // Take prevents JSON's trailing-data check from consuming the next record.
    let record: JournalRecord = serde_json::from_reader(reader.take(size)).map_err(|error| {
        ConnectorError::InvalidOutbox {
            path: path.display().to_string(),
            message: format!("record at byte {offset}: {error}"),
        }
    })?;
    match record {
        JournalRecord::Put { delivery } => Ok(*delivery),
        JournalRecord::Ack { .. } => Err(ConnectorError::InvalidOutbox {
            path: path.display().to_string(),
            message: format!("pending index at byte {offset} points to an ACK"),
        }),
    }
}
fn read_error(path: &Path, source: std::io::Error) -> ConnectorError {
    ConnectorError::OpenOutbox {
        path: path.display().to_string(),
        source,
    }
}
