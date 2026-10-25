//! Bounded actual original custody, not a completed-query history.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;
pub(super) struct NativeUse {
    pub(super) task: TaskJoinWitness,
    pub(super) cancel: CancellationToken,
    pub(super) original: CarrierAdoption,
}
impl Provider {
    pub(in crate::component::native_dns) async fn reap(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        #[cfg(feature = "dns")]
        self.reap_quic(end).await?;
        let entries = self.users.lock().expect("DNS users poisoned").clone();
        for entry in entries {
            // A live query can still be dialing or reading. Its completed
            // original may also be reclaimed during the same task's fallback.
            if !entry.task.is_joined() && entry.original.closed().is_none() {
                continue;
            }
            let report = entry
                .original
                .retire(end)
                .await
                .map_err(std::io::Error::other)?;
            if !report.is_complete()
                || !entry
                    .original
                    .closed()
                    .is_some_and(|closed| closed.matches(&entry.original))
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "DNS original custody remains unconfirmed",
                ));
            }
            // This provider opens native TCP/UDP with no independent driver or
            // foreign loan. Their actual controls have no task-report history;
            // query failures remain in the real host TaskScope.
            let retired = {
                let mut users = self.users.lock().expect("DNS users poisoned");
                users
                    .iter()
                    .position(|old| Arc::ptr_eq(old, &entry))
                    .map(|index| users.swap_remove(index))
            };
            drop(retired);
        }
        Ok(())
    }
}
