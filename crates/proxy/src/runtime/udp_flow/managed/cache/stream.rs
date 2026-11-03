use super::super::connection::SharedManagedUdpConnection;
use super::key::ManagedUdpConnectionCacheKey;
use std::collections::HashMap;
#[derive(Default)]
pub(crate) struct ManagedUdpConnectionCache {
    entries: HashMap<ManagedUdpConnectionCacheKey, SharedManagedUdpConnection>,
}
impl ManagedUdpConnectionCache {
    pub(crate) fn get<T: 'static>(&self, key: &str) -> Option<SharedManagedUdpConnection> {
        self.entries
            .get(&ManagedUdpConnectionCacheKey::new::<T>(key))
            .cloned()
    }
    pub(crate) fn insert<T: 'static>(
        &mut self,
        key: String,
        connection: SharedManagedUdpConnection,
    ) {
        self.entries
            .insert(ManagedUdpConnectionCacheKey::new::<T>(key), connection);
    }
}
