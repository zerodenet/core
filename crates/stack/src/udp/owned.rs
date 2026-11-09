//! Kernel input moves its already-paid complete IP allocation. Its UDP range
//! consumes no second Native unit and is held through the actual next user.
use super::*;
impl UserUdpStack {
    pub async fn feed_owned_with_peer(
        &self,
        packet: zero_traits::PacketBuffer,
        peer_identity: Option<Arc<str>>,
    ) {
        let Some(datagram) = packet::parse_udp(&packet) else {
            return;
        };
        if datagram.payload.is_empty() {
            return;
        }
        let source = endpoint_to_sockaddr(&datagram.src);
        let destination = endpoint_to_sockaddr(&datagram.dst);
        let start = datagram.payload.as_ptr() as usize - packet.as_ptr() as usize;
        let range = start..start + datagram.payload.len();
        let Some(memory) = self.outbound.packet_memory_view() else {
            warn!("owned UDP Source lacks its original memory view");
            return;
        };
        let _notifications = memory.defer_notifications();
        let mut rows = self
            .datagrams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if rows.len() >= 1024 {
            warn!("UDP datagram queue full, rejecting new packet");
            return;
        }
        let data = match self.outbound.retain_received_payload(packet, range) {
            Ok(data) => data,
            Err((cause, _original)) => {
                warn!(?cause, "UDP range metadata refused new packet");
                return;
            }
        };
        if rows
            .push(
                UdpStackDatagram {
                    peer_identity,
                    data,
                    source,
                    destination,
                },
                Some(&memory),
            )
            .is_err()
        {
            warn!("UDP queue metadata refused new row");
            return;
        }
        drop(rows);
        self.available.notify_one();
    }
}

#[cfg(test)]
mod tests;
