//! An actual existing tuple is held through the caller's protocol commit.
//! No callback, new task, route/peer inference or TCP mutation on preparation.
use super::*;
/// This is only a prepared original Stack consumer. It proves no authentication,
/// native close, actor join or Scope/publication receipt.
pub struct ClientTcpControlReceive<'a> {
    // Unlock the actual table before deferred memory notifications are sent.
    connections: tokio::sync::MutexGuard<'a, HashMap<ConnKey, Conn>>,
    #[cfg(feature = "packet-memory")]
    _notifications: Option<crate::packet_output::memory::PacketSendNotifications>,
    stack: &'a ClientTcpStack,
    packet: &'a [u8],
}
impl ClientTcpStack {
    pub async fn prepare_correlated_control<'a>(
        &'a self,
        raw_packet: &'a [u8],
    ) -> Option<ClientTcpControlReceive<'a>> {
        if packet::parse_ip_fragment(raw_packet).is_some() {
            return None;
        }
        let tcp = packet::parse_tcp(raw_packet)?;
        if !tcp.payload.is_empty() || !(tcp.ack_flag || tcp.rst || tcp.fin) {
            return None;
        }
        let key = key_from_parsed(&tcp);
        let connections = self.connections.lock().await;
        if !connections.contains_key(&key) {
            return None;
        }
        #[cfg(feature = "packet-memory")]
        let notifications = self.outbound.defer_memory_notifications();
        Some(ClientTcpControlReceive {
            connections,
            #[cfg(feature = "packet-memory")]
            _notifications: notifications,
            stack: self,
            packet: raw_packet,
        })
    }
}
#[cfg(feature = "packet-memory")]
impl ClientTcpStack {
    /// Prepare the original tuple and the actual application receive storage
    /// before a protocol commits replay/session/timers. TX is not borrowed for
    /// authenticated receive data. This is only the same Native memory lane.
    pub async fn prepare_correlated_owned<'a>(
        &'a self,
        raw_packet: &'a [u8],
        read: &zero_tun::packet_memory::NativeReadGrant,
    ) -> Result<Option<ClientTcpControlReceive<'a>>, zero_tun::packet_memory::PacketMemoryError>
    {
        use zero_tun::packet_memory::PacketMemoryError;
        let actual = read.memory_view()?;
        if !self
            .outbound
            .packet_memory_view()
            .is_some_and(|bound| bound.same_original(&actual))
        {
            return Err(PacketMemoryError::Stopped);
        }
        if packet::parse_ip_fragment(raw_packet).is_some() {
            return Ok(None);
        }
        let Some(tcp) = packet::parse_tcp(raw_packet) else {
            return Ok(None);
        };
        let key = key_from_parsed(&tcp);
        let locked = self.connections.lock().await;
        let notifications = self.outbound.defer_memory_notifications();
        // Declare the actual table guard after deferral, so every early error
        // unlocks it before releasing even another thread's deferred wake.
        let connections = locked;
        let Some(conn) = connections.get(&key) else {
            return Ok(None);
        };
        if !conn.receive_buffer.has_owned_storage() {
            return Err(PacketMemoryError::Stopped);
        }
        // Its actual finite ring was admitted before this connection's SYN.
        // No allocation or copy happens before the protocol's final commit;
        // subsequent consume uses the normal actual TCP window/sequence rules.
        Ok(Some(ClientTcpControlReceive {
            connections,
            _notifications: notifications,
            stack: self,
            packet: raw_packet,
        }))
    }
}
impl ClientTcpControlReceive<'_> {
    /// Call only after the real protocol's final authentication/replay/storage
    /// commit. Preparation and Drop alone make no application/TCP progress.
    pub fn consume(mut self) -> bool {
        self.stack
            .feed_correlated_locked(&mut self.connections, self.packet)
    }
}

#[cfg(all(test, feature = "packet-memory"))]
#[path = "tests/control_receive.rs"]
mod tests;
