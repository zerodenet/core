//! Original stack tables and control queue only; never a native/join receipt.
use super::*;
impl UserTcpStack {
    pub async fn quiescent(&self) -> bool {
        if !self.connections.lock().await.is_empty() {
            return false;
        }
        let queue = self.accept_queue.lock().await;
        queue.pending.is_none()
            && queue.receiver.is_empty()
            && self.control_packets.pending_packets() == 0
    }
}
