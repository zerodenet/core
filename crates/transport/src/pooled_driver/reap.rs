//! Reclaim only actual returned original records; never block a different live binding.
use super::ManagedPool;
use std::sync::atomic::Ordering;
use zero_platform_tokio::contract::ChannelError;
impl ManagedPool {
    /// Retire one actual exclusive cached original. Idle expiry is not a
    /// logical handoff; its replacement waits for this exact native receipt.
    pub async fn retire_connection(
        &self,
        record: &std::sync::Arc<super::Physical>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ChannelError> {
        if !record
            .owner
            .upgrade()
            .is_some_and(|owner| std::ptr::eq(owner.as_ref(), self))
        {
            return Err(super::failure(
                self.instance,
                zero_platform_tokio::contract::Stage::Close,
                zero_platform_tokio::contract::Reason::StalePlan,
            ));
        }
        record.stop.cancel();
        self.settle_failed(record, deadline).await?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            record.receive_available();
            if record.confirmed_closed() {
                self.harvest();
                return Ok(true);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Ok(false);
            }
        }
    }
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

impl ManagedPool {
    /// Actual nested loans use strictly shorter physical bindings. Stop all
    /// records first, then confirm each consumer before retiring its donor.
    /// Ordering is not proof: an incomplete original blocks every shorter one.
    pub(super) async fn retire_originals(
        &self,
        mut records: Vec<std::sync::Arc<super::Physical>>,
        deadline: tokio::time::Instant,
    ) -> Result<bool, ChannelError> {
        records.sort_by_key(|record| std::cmp::Reverse(record.binding.prefix.len()));
        for record in &records {
            record.stop.cancel();
        }
        for record in &records {
            if !self.retire_connection(record, deadline).await? {
                return Ok(false);
            }
        }
        self.harvest();
        Ok(self.records.lock().unwrap().is_empty())
    }
}
