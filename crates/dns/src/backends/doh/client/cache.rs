//! The same cache entries retire only outside their original membership lock.
use super::*;
impl DohDnsResolver {
    /// `None` dimensions do not change the original selection predicate. The
    /// original connect_lock still serializes replacement, not this reader.
    /// Keep only existing State Arcs, never a second cache or O(N) snapshot.
    pub(super) async fn retire_cached_clients(
        &self,
        replacing: Option<SocketAddr>,
        generation: Option<u64>,
    ) {
        let mut previous: Option<Arc<ClientState>> = None;
        loop {
            let original = {
                let clients = self.clients.lock().await;
                // Removal preserves the original Vec order. Re-find the exact
                // preceding original after every unlock; concurrent retirement
                // cannot shift an unseen entry behind an obsolete numeric cursor.
                let next = previous
                    .as_ref()
                    .and_then(|previous| {
                        clients
                            .iter()
                            .position(|entry| Arc::ptr_eq(&entry.state, previous))
                    })
                    .map_or(0, |index| index + 1);
                clients
                    .get(next)
                    .map(|entry| (entry.key.addr, entry.key.generation, entry.state.clone()))
            };
            let Some((address, actual_generation, state)) = original else {
                break;
            };
            // ensure_current may retire/cancel, call the real detour binding,
            // or wake an external I/O task synchronously. No clients lock here.
            let discard = replacing == Some(address)
                || generation.is_some_and(|generation| {
                    actual_generation != generation || !state.is_reusable()
                });
            if discard {
                let retired = {
                    let mut clients = self.clients.lock().await;
                    clients
                        .iter()
                        .position(|entry| Arc::ptr_eq(&entry.state, &state))
                        .map(|index| clients.remove(index))
                };
                // DohClient::drop retires its State and drops the original H2
                // sender. Cancellation and final sender Drop can invoke wakes.
                drop(retired);
            } else {
                // Even this previous observation's last Arc leaves outside the
                // cache lock. If another reader removed it, the next pass starts
                // at the current beginning, never evicting a new replacement.
                previous = Some(state);
            }
        }
    }
}
#[cfg(test)]
mod tests;
