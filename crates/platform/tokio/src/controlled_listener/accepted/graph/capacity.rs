//! Passive view/join observation, not an ACK or a native-close provider.
use super::*;
impl Users {
    pub(in crate::controlled_listener::accepted) async fn wait_complete(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.complete() {
                return;
            }
            changed.await;
        }
    }
}
