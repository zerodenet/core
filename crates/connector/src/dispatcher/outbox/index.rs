//! Disposable disk index. JSONL alone owns the durable delivery obligation.
//!
//! No event payload or per-event key remains in a Rust collection. SQLite's
//! page cache can spill to its private scratch file, unlike `:memory:`.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::{ConnectorError, ConnectorResult};

mod cursor;
#[cfg(test)]
#[path = "tests/index.rs"]
mod tests;

pub(super) struct PendingIndex {
    db: Connection,
    // Explicit file rather than SQLite's empty-name temporary database:
    // Android's bundled SQLite forces temporary databases into memory.
    // Field order closes SQLite before removing its private directory.
    _scratch: tempfile::TempDir,
    path: PathBuf,
    len: usize,
    batch_records: usize,
}

impl PendingIndex {
    pub(super) fn new(path: &Path) -> ConnectorResult<Self> {
        let scratch = tempfile::Builder::new()
            .prefix("zero-outbox-index-")
            .tempdir()
            .map_err(|e| error(path, e))?;
        let db =
            Connection::open(scratch.path().join("index.sqlite")).map_err(|e| error(path, e))?;
        db.execute_batch(
            "PRAGMA temp_store=FILE;
             PRAGMA cache_size=-256;
             PRAGMA cache_spill=ON;
             PRAGMA mmap_size=0;
             PRAGMA synchronous=OFF;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE pending (
                 sink TEXT NOT NULL, event TEXT NOT NULL, offset INTEGER NOT NULL,
                 PRIMARY KEY(sink,event)
             ) WITHOUT ROWID;
             CREATE UNIQUE INDEX pending_offset ON pending(offset);
             CREATE TABLE sink_counts (
                 sink TEXT PRIMARY KEY NOT NULL, count INTEGER NOT NULL
             ) WITHOUT ROWID;",
        )
        .map_err(|e| error(path, e))?;
        Ok(Self {
            db,
            _scratch: scratch,
            path: path.to_owned(),
            len: 0,
            batch_records: 0,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn counts_for<'a>(
        &self,
        sinks: impl IntoIterator<Item = &'a str>,
    ) -> ConnectorResult<BTreeMap<String, usize>> {
        let mut counts = BTreeMap::new();
        let mut statement = self
            .db
            .prepare_cached("SELECT count FROM sink_counts WHERE sink=?1")
            .map_err(|e| self.error(e))?;
        for sink in sinks {
            let count: Option<i64> = statement
                .query_row([sink], |r| r.get(0))
                .optional()
                .map_err(|e| self.error(e))?;
            if let Some(count) = count {
                counts.insert(
                    sink.to_owned(),
                    usize::try_from(count).map_err(|e| self.error(e))?,
                );
            }
        }
        Ok(counts)
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> ConnectorResult<BTreeMap<String, usize>> {
        let mut statement = self
            .db
            .prepare("SELECT sink,count FROM sink_counts")
            .map_err(|e| self.error(e))?;
        let mut rows = statement.query([]).map_err(|e| self.error(e))?;
        let mut counts = BTreeMap::new();
        while let Some(row) = rows.next().map_err(|e| self.error(e))? {
            let count: i64 = row.get(1).map_err(|e| self.error(e))?;
            counts.insert(
                row.get(0).map_err(|e| self.error(e))?,
                usize::try_from(count).map_err(|e| self.error(e))?,
            );
        }
        Ok(counts)
    }

    // Replay/compaction use bounded transactions. A giant transaction keeps
    // dirty-page/rollback metadata alive even with a small page cache.
    pub(super) fn begin_batch(&mut self) -> ConnectorResult<()> {
        self.db.execute_batch("BEGIN").map_err(|e| self.error(e))?;
        self.batch_records = 0;
        Ok(())
    }

    pub(super) fn finish_batch(&mut self) -> ConnectorResult<()> {
        self.db.execute_batch("COMMIT").map_err(|e| self.error(e))
    }

    fn advanced(&mut self) -> ConnectorResult<()> {
        if !self.db.is_autocommit() {
            self.batch_records += 1;
            if self.batch_records == 128 {
                self.finish_batch()?;
                self.begin_batch()?;
            }
        }
        Ok(())
    }

    pub(super) fn insert(&mut self, sink: &str, event: &str, offset: u64) -> ConnectorResult<()> {
        let offset = i64::try_from(offset).map_err(|e| self.error(e))?;
        let existed = self
            .db
            .query_row(
                "SELECT 1 FROM pending WHERE sink=?1 AND event=?2",
                params![sink, event],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| self.error(e))?
            .is_some();
        self.db
            .prepare_cached(
                "INSERT INTO pending VALUES (?1,?2,?3)
                 ON CONFLICT(sink,event) DO UPDATE SET offset=excluded.offset",
            )
            .and_then(|mut s| s.execute(params![sink, event, offset]))
            .map_err(|e| self.error(e))?;
        if !existed {
            self.len += 1;
            self.db.prepare_cached(
                "INSERT INTO sink_counts VALUES (?1,1) ON CONFLICT(sink) DO UPDATE SET count=count+1"
            ).and_then(|mut s| s.execute([sink])).map_err(|e| self.error(e))?;
        }
        self.advanced()
    }

    pub(super) fn remove(&mut self, sink: &str, event: &str) -> ConnectorResult<()> {
        let removed = self
            .db
            .prepare_cached("DELETE FROM pending WHERE sink=?1 AND event=?2")
            .and_then(|mut s| s.execute(params![sink, event]))
            .map_err(|e| self.error(e))?;
        if removed != 0 {
            self.len -= 1;
            self.db
                .prepare_cached("UPDATE sink_counts SET count=count-1 WHERE sink=?1")
                .and_then(|mut s| s.execute([sink]))
                .map_err(|e| self.error(e))?;
            self.db
                .prepare_cached("DELETE FROM sink_counts WHERE sink=?1 AND count=0")
                .and_then(|mut s| s.execute([sink]))
                .map_err(|e| self.error(e))?;
        }
        self.advanced()
    }

    fn error(&self, e: impl std::fmt::Display) -> ConnectorError {
        error(&self.path, e)
    }

    #[cfg(test)]
    pub(super) fn reject_writes(&self) {
        self.db
            .execute_batch(
                "CREATE TRIGGER fail_put BEFORE INSERT ON pending
             BEGIN SELECT RAISE(ABORT,'injected scratch index failure'); END;
             CREATE TRIGGER fail_ack BEFORE DELETE ON pending
             BEGIN SELECT RAISE(ABORT,'injected scratch index failure'); END;",
            )
            .unwrap();
    }

    #[cfg(test)]
    pub(super) fn cap_disk_pages(&self) {
        let pages: i64 = self
            .db
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        self.db
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
    }
}

fn error(path: &Path, e: impl std::fmt::Display) -> ConnectorError {
    // Never classify a scratch-index/storage failure as corrupt JSONL. The
    // caller must retain the journal and retry/rebuild, not quarantine it.
    ConnectorError::OutboxIndex {
        path: path.display().to_string(),
        message: e.to_string(),
    }
}
