//! Passive wait on this actual native custody book; not an acquisition grant.
use super::*;
impl CarrierOriginalTotals {
    /// Wait for a real refund in this same book. This neither acquires a
    /// reservation nor progresses native/Scope retirement on the waiter's behalf.
    pub async fn wait_capacity(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.lock();
                if state.charged() < state.limit.get() {
                    return;
                }
            }
            changed.await;
        }
    }
}
