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
        let domain = if !self
            .task
            .flow_users()
            .is_some_and(|users| users.quiescent())
        {
            Some("logical_users")
        } else if !self.returns.quiescent() {
            Some("packet_returns")
        } else if self.forwarded_packets.capacity() != self.forwarded_packets.max_capacity() {
            Some("forwarded_packets")
        } else if !self.udp.quiescent() {
            Some("udp_sockets_or_fragments")
        } else if !self.tcp.quiescent().await {
            Some("tcp_connections_or_control")
        } else {
            None
        };
        if let Some(domain) = domain {
            tracing::debug!(
                incarnation = self.incarnation,
                domain,
                "Packet stack quiescence remains charged"
            );
        }
        domain.is_none()
    }
}
