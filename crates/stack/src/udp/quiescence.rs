use super::*;

impl UserUdpStack {
    /// Existing receive queue only, observed while its actual producer pauses.
    pub async fn quiescent(&self) -> bool {
        self.datagrams.lock().await.is_empty()
    }
}
