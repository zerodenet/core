use super::*;

impl UserUdpStack {
    /// Existing receive queue only, observed while its actual producer pauses.
    pub async fn quiescent(&self) -> bool {
        self.datagrams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }
}
