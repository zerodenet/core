//! Bounded delivery custody in the same actual installed Resource.
use super::*;

pub(super) enum Phase {
    Driver,
    Shutdown,
}
impl Resource {
    pub(super) fn receive_cleanup_failure(&self, error: ChannelError, phase: Phase) -> SharedError {
        let actual = Arc::new(error);
        let phase = match phase {
            Phase::Driver => &self.exit_fault,
            Phase::Shutdown => &self.shutdown_fault,
        };
        // One actual controller publishes one stable finite cleanup payload;
        // subsequent causes belong to that payload, never a retry history.
        // The two unreceipted observation phases have separate fixed slots.
        let slot = if actual.cleanup_failure_receipt().is_some() {
            &self.cleanup_fault
        } else {
            phase
        };
        let mut retained = slot.get_or_init(|| actual.clone()).clone();
        if let Some(receipt) = actual.cleanup_failure_receipt() {
            if !same_receipt(&retained, receipt) {
                // A different real phase cannot replace the canonical fault.
                // Retain its exact Arc in this actual phase's remaining slot.
                retained = phase.get_or_init(|| actual.clone()).clone();
            }
            if same_receipt(&retained, receipt) {
                // The fixed Resource row already strongly retains this exact
                // delivered Arc and its producer's stable cause payload.
                // This ACK is neither native close, user return nor Scope ACK.
                receipt.acknowledge_retained(&retained);
            } else {
                // No matching bounded receiver: leave the original controller
                // and its unacknowledged receipt responsible for cleanup.
                return actual;
            }
        }
        retained
    }
    pub(super) fn cleanup_failures(&self) -> impl Iterator<Item = SharedError> + '_ {
        [&self.exit_fault, &self.shutdown_fault, &self.cleanup_fault]
            .into_iter()
            .filter_map(|fault| fault.get().cloned())
    }
}
fn same_receipt(
    source: &SharedError,
    receipt: &zero_platform_tokio::contract::CleanupFailureReceipt,
) -> bool {
    source
        .cleanup_failure_receipt()
        .is_some_and(|actual| actual.same_receipt(receipt))
}
