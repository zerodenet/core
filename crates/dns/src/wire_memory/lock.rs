//! Defers this DNS storage's metadata refunds until its original lock is out.
//! It wraps only existing DNS coordinator/flight/dependency mutexes; no queue,
//! task, memory book or allocation/admission authority is created here.
use super::*;
use std::sync::{LockResult, Mutex, MutexGuard, OnceLock, PoisonError};
use zero_platform_tokio::contract::component::PacketRootNotifications;
pub(crate) struct MetadataMutex<T> {
    value: Mutex<T>,
    metadata: OnceLock<PacketDriverMetadataView>,
}
pub(crate) struct MetadataGuard<'a, T> {
    // Declaration order is intentional: free all held values/guard first,
    // then let the same Root deliver its deferred refund notifications.
    value: MutexGuard<'a, T>,
    _notifications: Option<PacketRootNotifications>,
}
impl<T> MetadataMutex<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Mutex::new(value),
            metadata: OnceLock::new(),
        }
    }
    pub(crate) fn bind(&self, metadata: Option<PacketDriverMetadataView>) {
        if let Some(metadata) = metadata {
            if let Err(metadata) = self.metadata.set(metadata) {
                assert!(
                    self.metadata.get().unwrap().same_root(&metadata),
                    "DNS storage bound to one actual Host metadata Root"
                );
            }
        }
    }
    pub(crate) fn metadata(&self) -> Option<PacketDriverMetadataView> {
        self.metadata.get().cloned()
    }
    pub(crate) fn lock(&self) -> LockResult<MetadataGuard<'_, T>> {
        let notifications = self.metadata.get().map(|view| view.notifications());
        match self.value.lock() {
            Ok(value) => Ok(MetadataGuard {
                value,
                _notifications: notifications,
            }),
            Err(error) => Err(PoisonError::new(MetadataGuard {
                value: error.into_inner(),
                _notifications: notifications,
            })),
        }
    }
}
impl<T: Default> Default for MetadataMutex<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}
impl<T> std::fmt::Debug for MetadataMutex<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsMetadataMutex").finish_non_exhaustive()
    }
}
impl<T> std::fmt::Debug for MetadataGuard<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsMetadataGuard").finish_non_exhaustive()
    }
}
impl<T> std::ops::Deref for MetadataGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T> std::ops::DerefMut for MetadataGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}
