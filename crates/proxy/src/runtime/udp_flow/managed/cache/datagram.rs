use super::super::connection::SharedManagedDatagramUdpConnection;
use super::key::ManagedUdpConnectionCacheKey;
use std::collections::HashMap;
#[derive(Default)]
pub(crate) struct ManagedDatagramConnectionCache {
    entries: HashMap<ManagedUdpConnectionCacheKey, SharedManagedDatagramUdpConnection>,
}
impl ManagedDatagramConnectionCache {
    pub(crate) fn get<T: 'static>(&self, key: &str) -> Option<SharedManagedDatagramUdpConnection> {
        self.entries
            .get(&ManagedUdpConnectionCacheKey::new::<T>(key))
            .cloned()
    }
    pub(crate) fn prune_closed(&mut self) {
        // Return only retired logical views. Original providers, rather than
        // cache eviction, remain authoritative for native replacement.
        self.entries.retain(|_, connection| !connection.is_closed());
    }
    pub(crate) fn insert<T: 'static>(
        &mut self,
        key: String,
        connection: SharedManagedDatagramUdpConnection,
    ) {
        self.entries
            .insert(ManagedUdpConnectionCacheKey::new::<T>(key), connection);
    }
}
