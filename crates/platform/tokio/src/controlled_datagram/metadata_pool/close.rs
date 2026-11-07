//! External owner cleanup only: never await a Query's own ACK inside its body.
use super::*;
impl MetadataPoolOwner {
    /// Close native/users first, then passively observe the same original
    /// registrations. The existing Host task collector remains the only exit
    /// consumer. Call only outside those tasks, using the original close end.
    pub async fn shutdown_after_registration_ack(
        &self,
        end: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        // Publish original native/user facts before any ACK wait. Unknown and
        // unconfirmed originals return their existing report without waiting.
        let report = self.shutdown()?;
        let last = self
            .shared
            .state
            .lock()
            .expect("metadata pool poisoned")
            .next;
        let mut previous = 0;
        loop {
            use std::ops::Bound::{Excluded, Included};
            let slot = self
                .shared
                .state
                .lock()
                .expect("metadata pool poisoned")
                .slots
                .range((Excluded(previous), Included(last)))
                .next()
                .map(|(ordinal, slot)| (*ordinal, slot.clone()));
            let Some((ordinal, slot)) = slot else {
                break;
            };
            previous = ordinal;
            if slot.closed.get().is_none() {
                return Ok(report);
            }
            let (caller, users) = {
                let native = slot.native()?;
                (slot.caller.get().cloned(), native.confirmed.clone())
            };
            let wait = async {
                if let Some(caller) = caller {
                    caller.wait_registration_released().await;
                }
                if let Some(users) = users {
                    for registration in users
                        .registrations()
                        .iter()
                        .chain(users.related_registrations())
                    {
                        registration.wait_registration_released().await;
                    }
                }
            };
            if tokio::time::timeout_at(end, wait).await.is_err() {
                // This Err is the actual observer deadline above, not an Err
                // returned by an IO/controller/driver. Keep the original full
                // partial report (including its ACK unfinished) unchanged.
                return Ok(report);
            }
            MetadataPoolPermit(slot).release_registration_credit();
        }
        // Only new positive observations supersede this owner's former
        // ACK-specific unfinished result. No protocol/task failures are erased.
        self.shutdown()
    }
}
