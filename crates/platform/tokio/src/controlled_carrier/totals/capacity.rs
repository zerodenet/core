//! Passive wait on this actual native custody book; not an acquisition grant.
use super::*;
impl CarrierOriginalTotals {
    /// Wait for a real refund in this same book. This neither acquires a
    /// reservation nor progresses native/Scope retirement on the waiter's behalf.
    pub async fn wait_capacity(&self) {
        self.wait_capacity_for(1).await;
    }
    pub(in crate::controlled_carrier) async fn wait_capacity_for(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.lock();
                if state.limit.get().saturating_sub(state.charged()) >= count {
                    return;
                }
            }
            changed.await;
        }
    }
}
