//! A bounded caller's logical returns over the existing global controller set.
//! No second registry, allocator, task, native authority or increased capacity.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::time::Instant;
use zero_platform_tokio::contract::lifecycle::Unfinished;

#[derive(Default)]
pub(crate) struct DatagramReturns {
    closing: AtomicBool,
    pending: AtomicUsize,
    settled: AtomicUsize,
    unacquired: AtomicUsize,
    changed: tokio::sync::Notify,
}
impl DatagramReturns {
    pub(super) fn is_closed(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }
    pub(super) fn reserve(&self) {
        self.pending.fetch_add(1, Ordering::AcqRel);
    }
    pub(super) fn finish(&self, acquired: bool) {
        if acquired {
            self.settled.fetch_add(1, Ordering::AcqRel);
        } else {
            self.unacquired.fetch_add(1, Ordering::AcqRel);
        }
        assert_ne!(
            self.pending.fetch_sub(1, Ordering::AcqRel),
            0,
            "only the original reservation can settle its return"
        );
        self.changed.notify_waiters();
    }
    fn report(&self) -> CloseReport {
        let mut report = CloseReport::default();
        let settled = self.settled.swap(0, Ordering::AcqRel);
        if settled != 0 {
            report.released_resources.push(format!(
                "{settled} original logical Datagram controllers settled"
            ));
        }
        let unacquired = self.unacquired.swap(0, Ordering::AcqRel);
        if unacquired != 0 {
            report.released_resources.push(format!(
                "{unacquired} unacquired logical Datagram reservations returned"
            ));
        }
        if self.pending.load(Ordering::Acquire) != 0 {
            report.unfinished.push(Unfinished::Resource(
                "original logical Datagram controllers remain owned".into(),
            ));
        }
        report
    }
}
impl RetiredDatagrams {
    pub(crate) async fn wait_returns(
        &self,
        returns: &Arc<DatagramReturns>,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        returns.closing.store(true, Ordering::Release);
        // Same original global entries and controllers; unrelated callers keep
        // their admission and their original shared global capacity limit.
        let entries = self.entries();
        for (id, entry) in entries {
            let belongs = entry
                .lock()
                .expect("Datagram entry poisoned")
                .returns
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, returns));
            if belongs {
                self.retire(id, &entry, CloseReason::Shutdown);
            }
        }
        while returns.pending.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
            let changed = returns.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if returns.pending.load(Ordering::Acquire) == 0 {
                break;
            }
            // Existing polling preserves actual controller failures and native
            // close reports. Foreign failures do not sign this caller's return.
            let result = tokio::select! {
                _ = changed => continue,
                result = self.progress_retired(deadline) => result,
            };
            if let Err(error) = result {
                let original = self.entries().into_iter().find_map(|(_, entry)| {
                    let entry = entry.lock().expect("Datagram entry poisoned");
                    if entry
                        .returns
                        .as_ref()
                        .is_some_and(|actual| Arc::ptr_eq(actual, returns))
                    {
                        entry.failure.clone()
                    } else {
                        None
                    }
                });
                if let Some(source) = original {
                    // Keep positive receipts in the counters across this error;
                    // the actual original controller/source stays in its Entry.
                    return Err(super::super::instance::retained(source));
                }
                tracing::warn!(error = %error, "Datagram controller cleanup remains owned");
            }
        }
        Ok(returns.report())
    }
}
