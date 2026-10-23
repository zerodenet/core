//! Caller-driven storage maintenance; no stack-owned timer or background task.
use super::ClientUdpStack;
use std::time::Instant;

impl ClientUdpStack {
    /// Fence bindings, fail pending receive waiters, and release sender ownership.
    /// This stack owns no native socket and issues no native close receipt.
    pub fn shutdown(&self) {
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        sockets.clear();
    }

    /// Expire incomplete fragment assemblies and release oversized port-table
    /// storage. Live sockets, queued replies and destination/MTU state remain.
    /// Returns the number of expired fragment assemblies.
    pub fn cleanup_expired(&self, now: Instant) -> usize {
        let expired = self
            .inner
            .fragments
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cleanup_expired(now);
        let mut sockets = self.inner.sockets.lock().unwrap_or_else(|e| e.into_inner());
        let live = sockets.len();
        if sockets.capacity() > live.saturating_mul(4).max(16) {
            sockets.shrink_to(live);
        }
        expired
    }
}

#[cfg(test)]
#[path = "../../tests/client_udp/maintenance.rs"]
mod tests;
