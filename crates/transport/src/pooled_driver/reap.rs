//! Reclaim only actual returned original records; never block a different live binding.
use super::ManagedPool;
use std::sync::{atomic::Ordering, Arc};
use zero_platform_tokio::contract::ChannelError;
impl ManagedPool {
    pub(super) fn harvest(&self) {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
        }
        let removed = {
            let mut records = self.records.lock().unwrap();
            let mut removed = Vec::new();
            let mut index = 0;
            while index < records.len() {
                if records[index].closed() {
                    removed.push(records.remove(index));
                } else {
                    index += 1;
                }
            }
            removed
        };
        // The final Arc can own protocol/native state. Never drop it while the
        // actual pool census mutex is held.
        drop(removed);
    }
    pub(super) fn reap_ready(&self) -> Result<(), ChannelError> {
        let records = self.records.lock().unwrap().clone();
        for record in records {
            if !record.opening.load(Ordering::Acquire) && record.done.load(Ordering::Acquire) {
                record.release_installed_pending();
                let native = record.original.try_retire_native()?;
                if native
                    .closed()
                    .is_some_and(|closed| closed.matches(&record.original))
                {
                    let report = record.original.try_retire()?;
                    self.settled.lock().unwrap().append(report);
                }
                let pending = record.pending.lock().unwrap().clone();
                for original in pending {
                    let report = original.try_retire()?;
                    self.settled.lock().unwrap().append(report);
                }
                record.release_installed_pending();
            }
        }
        self.harvest();
        Ok(())
    }
}
