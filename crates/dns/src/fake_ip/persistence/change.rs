//! Preflight the complete next checkpoint before appending any part of a change.
use super::super::checkpoint::Checkpoint;
use super::*;
impl FakeIpPersistence {
    pub(in crate::fake_ip) fn commit_change(
        &self,
        checkpoint: &Checkpoint<'_>,
        records: &[codec::RecordRef<'_>],
    ) -> io::Result<()> {
        let _call = self.lease.begin()?;
        let checkpoint_bytes = codec::measured(|writer| checkpoint.encode(&self.metadata, writer))?;
        let append_bytes = codec::measured(|writer| {
            for record in records {
                write_record(writer, record)?;
            }
            Ok(())
        })?;
        let grant = self.lease.grant()?;
        // A new state which cannot be restarted is rejected before any old
        // live mapping or valid retirement history is changed or evicted.
        if checkpoint_bytes > grant.max_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "complete live Fake-IP checkpoint exceeds recovery envelope",
            ));
        }
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_current(&journal, &grant)?;
        let total = journal.bytes.checked_add(append_bytes).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::FileTooLarge,
                "Fake-IP append length overflow",
            )
        })?;
        let replace = total > grant.max_bytes();
        drop(journal);
        if replace {
            grant.replace(checkpoint_bytes, |writer| {
                checkpoint.encode(&self.metadata, writer)
            })?;
        } else {
            grant.append(append_bytes, |writer| {
                for record in records {
                    write_record(writer, record)?;
                }
                writer.flush()
            })?;
        }
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if replace {
            journal.bytes = checkpoint_bytes;
            journal.records = checkpoint.records();
        } else {
            journal.bytes = total;
            journal.records = journal.records.saturating_add(records.len());
        }
        Ok(())
    }
    pub(in crate::fake_ip) fn compact_checkpoint(
        &self,
        checkpoint: &Checkpoint<'_>,
    ) -> io::Result<()> {
        let _call = self.lease.begin()?;
        let bytes = codec::measured(|writer| checkpoint.encode(&self.metadata, writer))?;
        let grant = self.lease.grant()?;
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_current(&journal, &grant)?;
        drop(journal);
        grant.replace(bytes, |writer| checkpoint.encode(&self.metadata, writer))?;
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.bytes = bytes;
        journal.records = checkpoint.records();
        Ok(())
    }
    pub(in crate::fake_ip) fn rebind_checkpoint(
        &mut self,
        checkpoint: &Checkpoint<'_>,
    ) -> io::Result<()> {
        let _call = self.lease.begin()?;
        let grant = self.lease.grant()?;
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = journal
            .generation
            .checked_add(1)
            .ok_or_else(lease::generation_exhausted)?;
        if !self.lease.matches(&grant) {
            return Err(stale());
        }
        let bytes = codec::measured(|writer| checkpoint.encode(&self.metadata, writer))?;
        drop(journal);
        grant.replace(bytes, |writer| checkpoint.encode(&self.metadata, writer))?;
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.generation = generation;
        journal.ready = true;
        journal.bytes = bytes;
        journal.records = checkpoint.records();
        self.generation = generation;
        Ok(())
    }
    pub(in crate::fake_ip) fn checkpoint_bytes(
        &self,
        checkpoint: &Checkpoint<'_>,
    ) -> io::Result<usize> {
        codec::measured(|writer| checkpoint.encode(&self.metadata, writer))
    }
    pub(in crate::fake_ip) fn check_checkpoint(
        &self,
        checkpoint: &Checkpoint<'_>,
    ) -> io::Result<()> {
        let bytes = codec::measured(|writer| checkpoint.encode(&self.metadata, writer))?;
        let grant = self.lease.grant()?;
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.ensure_current(&journal, &grant)?;
        if bytes > grant.max_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                "complete live Fake-IP checkpoint exceeds recovery envelope",
            ));
        }
        Ok(())
    }
}
