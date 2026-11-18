//! Original stack tables and control queue only; never a native/join receipt.
use super::*;
impl UserTcpStack {
    pub async fn quiescent(&self) -> bool {
        let connections = self.connections.lock().await;
        if !connections.is_empty() {
            tracing::debug!(
                connections = connections.len(),
                "TCP stack still owns original connections during pause"
            );
            return false;
        }
        drop(connections);
        let queue = self.accept_queue.lock().expect("TCP accept queue poisoned");
        let idle = queue.pending.is_none()
            && queue.receiver.is_empty()
            && self.control_packets.pending_packets() == 0;
        if !idle {
            tracing::debug!(
                accepted_pending = queue.pending.is_some(),
                accepted_queued = queue.receiver.len(),
                control_pending = self.control_packets.pending_packets(),
                "TCP stack still owns queued work during pause"
            );
        }
        idle
    }
}
