//! Dynamic ECH addresses share this exact pool and its original slot budget.
use super::*;
use std::sync::atomic::Ordering;
impl DohDnsResolver {
    // The caller holds the existing connect_lock. A different answer never
    // cancels another request just to obtain its native slot.
    pub(super) async fn make_candidate_room(
        &self,
        peers: &[SocketAddr],
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        loop {
            let changed = self.owner.original_changes().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.owner.ensure_open()?;
            let full = self.owner.native_at_capacity();
            let (retired, incompatible_user) = {
                let mut clients = tokio::time::timeout_at(end, self.clients.lock())
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                let incompatible_user = clients.iter().any(|client| {
                    !peers.contains(&client.key.addr)
                        && client.state.leases.load(Ordering::Acquire) != 0
                });
                let idle = clients.iter().position(|client| {
                    (full || !peers.contains(&client.key.addr))
                        && client.state.leases.load(Ordering::Acquire) == 0
                });
                (idle.map(|index| clients.remove(index)), incompatible_user)
            };
            // Cancellation may invoke external wakers; drop the cached actual
            // controller outside its cache lock, then consume the same joins.
            let was_retired = retired.is_some();
            drop(retired);
            self.owner.drain_retired(end).await?;
            if was_retired {
                continue;
            }
            if !self.owner.native_at_capacity() && !incompatible_user {
                return Ok(());
            }
            tokio::time::timeout_at(end, changed)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        }
    }
}
