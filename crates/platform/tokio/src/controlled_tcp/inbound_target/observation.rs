//! Exact original close observations and bounded consumed report deltas.
use super::*;
/// Same graph's positive native/view/real-join proof. HostACK is independent.
#[derive(Clone)]
pub struct InboundTargetClosed(pub(super) Arc<Core>);
impl InboundTargetClosed {
    pub fn matches(&self, owner: &InboundTargetNativeOwner) -> bool {
        Arc::ptr_eq(&self.0, &owner.0)
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.0.acquired.load(Ordering::Acquire)
    }
    pub fn native_originals_closed(&self) -> usize {
        self.0
            .state
            .try_lock()
            .map_or(0, |state| state.total_confirmed)
    }
    pub fn registration_released(&self) -> bool {
        self.0.acked()
    }
}

impl State {
    pub(super) fn collect(&mut self, index: usize, mut report: CloseReport) -> Option<Slot> {
        let pending = std::mem::take(&mut report.unfinished);
        if !pending.is_empty() {
            self.pending.insert(index, pending);
            return None;
        }
        self.pending.remove(&index);
        // These are deltas consumed from each exact native controller, not
        // guessed socket counts or a parsed diagnostic string.
        self.total_confirmed = self
            .total_confirmed
            .checked_add(report.closed_io.len())
            .expect("confirmed native receipts cannot exceed original identities");
        self.confirmed = self
            .confirmed
            .checked_add(report.closed_io.len())
            .expect("confirmed native receipts cannot exceed original identities");
        let mut original = self.slots.remove(&index)?;
        if let Some(permit) = original.permit.take() {
            // The same graph may reuse a physically confirmed candidate slot.
            // Another graph receives no local credit before this actor's ACK.
            self.idle_permits.push(permit);
        }
        Some(original)
    }
    pub(super) fn report(&mut self) -> CloseReport {
        let mut report = CloseReport::default();
        let confirmed = std::mem::take(&mut self.confirmed);
        if confirmed != 0 {
            report
                .closed_io
                .push(format!("{confirmed} exact target native close receipts"));
        }
        report.unfinished = self.pending.values().flatten().cloned().collect();
        if !self.slots.is_empty() && report.unfinished.is_empty() {
            report.unfinished.push(Unfinished::Resource(
                "target originals remain pending".into(),
            ));
        }
        report
    }
}
