//! Dynamic ECH addresses share this exact pool and its original slot budget.
use super::*;
use std::sync::atomic::Ordering;
impl DohDnsResolver {
    // The caller holds the existing connect_lock. A different answer never
    // cancels another request just to obtain its native slot.
    pub(super) async fn make_candidate_room(&self, end: tokio::time::Instant) -> io::Result<()> {
        loop {
            let changed = self.owner.original_changes().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.owner.ensure_open()?;
            if !self.owner.native_at_capacity() {
                return Ok(());
            }
            let retired = {
                let mut clients = tokio::time::timeout_at(end, self.clients.lock())
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                let idle = clients
                    .iter()
                    .position(|client| client.state.leases.load(Ordering::Acquire) == 0);
                idle.map(|index| clients.remove(index))
            };
            // Cancellation may invoke external wakers; drop the cached actual
            // controller outside its cache lock, then consume the same joins.
            let was_retired = retired.is_some();
            drop(retired);
            self.owner.drain_retired(end).await?;
            if was_retired {
                continue;
            }
            if !self.owner.native_at_capacity() {
                return Ok(());
            }
            tokio::time::timeout_at(end, changed)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        }
    }
}
