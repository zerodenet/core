//! Finite pure sources from the actual accepted path RX set.
use super::*;
#[derive(Default)]
pub(super) struct Failures {
    // One cell per original retained logical controller, not per retry.
    pub(super) paths: Mutex<PacketMetadataVec<PacketMetadataArc<PathFailure>>>,
    pub(super) first: std::sync::OnceLock<RetainedError>,
}
pub(super) struct PathFailure {
    pub(super) original: std::sync::OnceLock<RetainedError>,
    pub(super) credit: Mutex<Option<PacketMetadataCredit>>,
}
/// Pure observation of this actual receive result, never a Scope/native receipt.
pub(in crate::runtime::udp_flow::packet_path_chain) struct PathStatus {
    pub(super) source: PacketMetadataArc<PathFailure>,
    pub(super) failures: PacketMetadataArc<Failures>,
    pub(super) memory: PacketDriverMetadataView,
}
impl PathStatus {
    pub(in crate::runtime::udp_flow::packet_path_chain) fn failure(&self) -> Option<RetainedError> {
        self.source.original.get().cloned()
    }
    pub(in crate::runtime::udp_flow::packet_path_chain) fn forget_closed(
        &self,
        original: &crate::component::datagram_retirement::DatagramRegistration,
    ) {
        // A failed row is the canonical pure capsule's source storage. Even
        // after local acceptance, deleting it would alter the receiver's same
        // payload and erase a later independent cause. Keep it in this actual
        // finite association cohort; only successful rows can be reclaimed.
        if self.source.original.get().is_some() || !original.closed() {
            return;
        }
        let notifications = self.memory.notifications();
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
        drop(notifications);
        // Failed source rows remain bounded by this original association's
        // declared max_paths; a full unresolved cohort returns Capacity.
    }
}

impl std::fmt::Debug for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketPathResponseSources")
            .field("has_original_failure", &self.first.get().is_some())
            .finish()
    }
}
impl std::fmt::Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original packet-path response failure(s)")
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.first.get().map(|source| &**source as _)
    }
}
