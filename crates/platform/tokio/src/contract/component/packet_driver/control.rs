use super::*;
use crate::contract::BoxFuture;
use std::sync::atomic::Ordering;
pub(super) struct StoredControl {
    pub(super) record: RecordRef,
    pub(super) installed: InstalledRef,
}
impl Installed {
    fn observe(&self, owner: &crate::controlled_carrier::loan::LoanOwner) {
        if owner.closed().is_some_and(|same| same.matches(owner)) {
            self.complete.store(true, Ordering::Release);
        }
    }
}
impl DynChannelControl for StoredControl {
    fn cancel(&self, _: CloseReason) {
        self.record.request_retirement();
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.record.request_retirement();
        match self.installed.control.try_lock() {
            Ok(mut control) => {
                self.installed.observe(&control);
                if let Some(source) = self
                    .record
                    .failed_phase(&self.record.failures.final_control)
                    .filter(|_| !control.logical_cleanup_continuation())
                {
                    return Err(source);
                }
                if self.installed.complete.load(Ordering::Acquire) {
                    return Ok(CloseReport::default());
                }
                let result = control.try_close();
                self.installed.observe(&control);
                // Persist while this same original's control mutex still
                // serializes the phase; another closer cannot issue a second
                // error before the first exact cause is retained.
                self.record
                    .receive_phase(&self.record.failures.final_control, result)
            }
            Err(_) => Ok(pending()),
        }
    }

    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.record.request_retirement();
            let Ok(mut control) =
                tokio::time::timeout_at(deadline, self.installed.control.lock()).await
            else {
                return Ok(pending());
            };
            self.installed.observe(&control);
            if let Some(source) = self
                .record
                .failed_phase(&self.record.failures.final_control)
                .filter(|_| !control.logical_cleanup_continuation())
            {
                return Err(source);
            }
            if self.installed.complete.load(Ordering::Acquire) {
                return Ok(CloseReport::default());
            }
            let result = control.close(deadline).await;
            self.installed.observe(&control);
            self.record
                .receive_phase(&self.record.failures.final_control, result)
        })
    }
}
impl Record {
    pub(super) fn request_retirement(&self) {
        // This is the record's child cancellation domain, never its caller's
        // original parent token. Wake callbacks run outside the pending lock.
        if let Some(installed) = self.installed.get() {
            self.release_host_retained_pending(installed);
        }
        self.call.cancel.cancel();
        for index in 0..self.max_pending {
            let Some(adoption) = self.pending_at(index) else {
                break;
            };
            adoption.request_retirement();
        }
        for index in 0..self.packet_originals.len() {
            let Some(stream) = self.stream_at(index) else {
                break;
            };
            stream.request_retirement();
        }
        if let Some(installed) = self.installed.get() {
            if let Ok(control) = installed.control.try_lock() {
                control.cancel(CloseReason::Shutdown);
            }
        }
    }
}
pub(super) fn pending() -> CloseReport {
    CloseReport {
        unfinished: vec![Unfinished::Resource(
            "same Packet driver controller remains pending".into(),
        )],
        ..Default::default()
    }
}
