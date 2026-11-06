//! Original client connections and retransmission/control payloads.
use super::*;
impl ClientTcpStack {
    pub async fn quiescent(&self) -> bool {
        self.connections.lock().await.is_empty() && self.control_packets.pending_packets() == 0
    }
}
