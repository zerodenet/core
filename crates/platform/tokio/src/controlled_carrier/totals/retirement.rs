//! Existing Host continuation for the same retained original rows.
use super::*;
impl CarrierOriginalTotals {
    /// Called inline by the existing Host supervisor. Cancelled originals use
    /// their existing persistent retirement continuation and immutable deadline.
    /// The root never revokes a live sibling, creates a task, or ACKs an exit.
    pub fn poll_cancelled_retirements(&self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        let source = self.lock().source.clone();
        for original in source {
            changed |= original.poll_retirement(cx);
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_source(&original);
                changed = true;
            }
        }
        let metadata = self.lock().metadata.clone();
        for original in metadata {
            changed |= original.poll_retirement(cx);
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_metadata(&original);
                changed = true;
            }
        }
        let bound = self.lock().bound.clone();
        for original in bound {
            changed |= original.poll_retirement(cx);
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release_bound(&original);
                changed = true;
            }
        }
        let originals = self.originals();
        let mut owners = std::collections::BTreeSet::new();
        for (_, original) in &originals {
            if owners.insert(original.owner_identity()) {
                changed |= original.poll_cancelled_owner(cx);
            }
        }
        for (id, original) in originals {
            if !original.released() {
                changed |= original.poll_ack(cx);
            }
            if original.received(original.failure(), original.resource_failure())
                && original.released()
            {
                self.release(id, &original);
                changed = true;
            }
        }
        changed
    }
}
