//! Same graph's actual users, virtual sockets and charged return originals.
use super::*;
impl SharedRawIpDevice {
    pub(crate) async fn quiescent(&self) -> bool {
        self.task
            .flow_users()
            .is_some_and(|users| users.quiescent())
            && self.returns.quiescent()
            && self.forwarded_packets.capacity() == self.forwarded_packets.max_capacity()
            && self.udp.quiescent()
            && self.tcp.quiescent().await
    }
}
