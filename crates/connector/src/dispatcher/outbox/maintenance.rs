use super::*;

impl DeliveryOutbox {
    pub(super) fn check_index(&self) -> ConnectorResult<()> {
        if self.index_valid {
            Ok(())
        } else {
            Err(ConnectorError::OutboxIndex {
                path: self.path.display().to_string(),
                message: "journal changed; derived index requires replay before use".into(),
            })
        }
    }

    pub(super) fn ensure_index(&mut self) -> ConnectorResult<()> {
        if !self.index_valid {
            // Open separately: after a successful rename followed by an open
            // failure, self.file may be None or refer to the old inode.
            let mut file = open_journal(&self.path, false)?;
            let (pending, records, recovery) = read_pending(&self.path, &mut file)?;
            self.pending = pending;
            self.journal_records = records;
            if recovery.is_some() {
                self.recovery = recovery;
            }
            self.file = Some(file);
            self.frame_cache.set(None);
            self.index_valid = true;
        }
        Ok(())
    }
}
