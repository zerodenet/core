//! Reclaim only actual returned original records; never block a different live binding.
use super::ManagedPool;
use std::sync::atomic::Ordering;
use zero_platform_tokio::contract::ChannelError;
impl ManagedPool {
    pub(super) fn harvest(&self) {
        let mut after = None;
        loop {
            let record = {
                let records = self.records.lock().unwrap();
                records
                    .iter()
                    .filter(|record| {
                        after.is_none_or(|after| std::sync::Arc::as_ptr(record) as usize > after)
                    })
                    .min_by_key(|record| std::sync::Arc::as_ptr(record) as usize)
                    .cloned()
            };
            let Some(record) = record else { break };
            after = Some(std::sync::Arc::as_ptr(&record) as usize);
            record.receive_available();
            if !record.closed() {
                continue;
            }
            let removed = {
                let mut records = self.records.lock().unwrap();
                records
                    .iter()
                    .position(|actual| std::sync::Arc::ptr_eq(actual, &record))
                    .map(|index| records.remove(index))
            };
            drop(removed);
        }
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
                if let Some(children) = &record.protocols {
                    children.request_return();
                    children.reclaim_returned();
                }
            }
        }
        self.harvest();
        Ok(())
    }
}
