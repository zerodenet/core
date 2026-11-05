//! Unpublished actual failures charge the same finite entry budget. Successes
//! consume no lifetime credit once their original native/user/task gates settle.
use super::*;
pub(super) struct FailedGroup {
    pub(super) drivers: Vec<DriverDelivery>,
    pub(super) application: Vec<zero_platform_tokio::contract::scope::TaskJoined>,
}
pub(super) fn append_report<'a>(
    report: &mut CloseReport,
    joined: impl IntoIterator<Item = &'a zero_platform_tokio::contract::scope::TaskJoined>,
) {
    for joined in joined {
        if let Err(source) = joined.result() {
            let mut forwarded = ChannelError::new(source.stage, source.reason, source.impact);
            forwarded.component = source.component;
            forwarded.session = source.session;
            forwarded.delivery = source.delivery;
            forwarded.retry = source.retry;
            report
                .failed_tasks
                .push(zero_platform_tokio::contract::lifecycle::TaskFailure {
                    id: joined.id(),
                    class: joined.class(),
                    error: forwarded.with_source(OriginalChannel(source)),
                });
        }
    }
}
impl EntryState {
    pub(super) fn has_failures(&self) -> bool {
        !self.retired_drivers.is_empty() || !self.retired_application.is_empty()
    }
    fn take_failures(&mut self) -> FailedGroup {
        FailedGroup {
            drivers: std::mem::take(&mut self.retired_drivers),
            application: std::mem::take(&mut self.retired_application),
        }
    }
}
impl Generation {
    pub(super) fn trace_budget(&self) {
        let (entries, failure_groups) = {
            let entries = self.entries.lock().unwrap();
            let failures = self.failures.lock().unwrap();
            (entries.len(), failures.len())
        };
        tracing::debug!(target: "hysteria2::pool_budget", entries, failure_groups, entry_limit=self.entry_limit.get(), "original pool budget");
    }
    /// A failed candidate's current original is fully settled first. Reserve
    /// room for that immutable group before the same active entry can construct
    /// another original. Refusal leaves the real sources in that same entry.
    pub(super) fn retain_failed_candidate(
        &self,
        state: &mut EntryState,
    ) -> Result<(), ChannelError> {
        if !state.has_failures() {
            return Ok(());
        }
        let entries = self.entries.lock().unwrap();
        let mut failures = self.failures.lock().unwrap();
        if entries.len() + failures.len() >= self.entry_limit.get() {
            return Err(error(Stage::Prepare, Reason::Capacity));
        }
        failures.push(state.take_failures());
        drop(failures);
        drop(entries);
        self.trace_budget();
        Ok(())
    }
    /// Transfer the exact closed entry's debt before removing its map owner.
    /// Keeping a new successor with the same key cannot remove that successor.
    pub(super) async fn remove_entry(
        &self,
        key: &Key,
        entry: &Arc<Entry>,
    ) -> Result<(), ChannelError> {
        let mut state = entry.state.lock().await;
        if !state.closed {
            return Err(error(Stage::Close, Reason::Internal));
        }
        let removed = {
            let mut entries = self.entries.lock().unwrap();
            if !entries
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, entry))
            {
                return Ok(());
            }
            if state.has_failures() {
                // Exchange this existing entry position for its actual settled
                // failure group. No additional capacity is created or refunded.
                self.failures.lock().unwrap().push(state.take_failures());
            }
            entries.remove(key)
        };
        drop(state);
        drop(removed);
        self.trace_budget();
        Ok(())
    }
}
