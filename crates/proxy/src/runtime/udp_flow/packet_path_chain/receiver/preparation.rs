//! Failure before the actual receive job is submitted returns only its memory.
use super::*;
impl Drop for PreparedReceive {
    fn drop(&mut self) {
        if self.submitted {
            return;
        }
        let _notifications = self.memory.notifications();
        let removed = {
            let mut paths = self
                .failures
                .paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            paths
                .iter()
                .position(|actual| PacketMetadataArc::ptr_eq(actual, &self.source))
                .map(|index| paths.remove(index))
        };
        drop(removed);
    }
}
