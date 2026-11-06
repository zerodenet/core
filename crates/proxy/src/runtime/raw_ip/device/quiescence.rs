//! Same graph's actual users, virtual sockets and charged return originals.
use super::*;
impl SharedRawIpDevice {
    /// Preflight only; normal quiescence still requires the real flows/sockets
    /// and queued return originals to end before native/Scope replacement.
    pub(crate) fn replacement_users_retained(
        &self,
        stopping: &crate::component::catalog::ReplacementUsersPause,
        source: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        self.task
            .flow_users()
            .is_some_and(|users| users.replacement_receivers_retained(stopping, source))
    }

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
