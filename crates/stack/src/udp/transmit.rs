//! The actual owned Source return uses the same UDP/fragment/TX algorithm.
use super::*;
impl UserUdpStack {
    pub(super) async fn send_datagram(&self, data: &[u8], src: SocketAddress, dst: SocketAddress) {
        let src_ip = sockaddr_to_ipaddr(&src);
        let dst_ip = sockaddr_to_ipaddr(&dst);
        let maximum_payload = if src_ip.is_ipv4() { 65_507 } else { 65_527 };
        if data.len() > maximum_payload {
            warn!(
                payload_bytes = data.len(),
                maximum_payload, "UDP payload exceeds the IP datagram limit"
            );
            return;
        }
        #[cfg(feature = "packet-memory")]
        if self.outbound.has_transmit_allocator() {
            let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
            let spec = packet::UdpPacketSpec {
                source: src_ip,
                destination: dst_ip,
                source_port: src.port,
                destination_port: dst.port,
                payload: data,
            };
            let fragments = loop {
                // Arm before admission. Existing valid replies keep their
                // original input/response owner while the same Root is busy.
                let changed = self.outbound.transmit_notification();
                tokio::pin!(changed);
                let ready = std::future::poll_fn(|cx| {
                    use std::future::Future;
                    std::task::Poll::Ready(changed.as_mut().poll(cx).is_ready())
                })
                .await;
                match crate::client_udp::transmit::build(
                    &self.outbound,
                    spec,
                    self.mtu,
                    identification,
                )
                .await
                {
                    Ok(packets) => break packets,
                    Err(
                        zero_traits::PacketDeriveError::Busy
                        | zero_traits::PacketDeriveError::Capacity
                        | zero_traits::PacketDeriveError::Revoked,
                    ) => {
                        if self
                            .outbound
                            .packet_memory_view()
                            .is_some_and(|view| view.check_open().is_err())
                        {
                            return;
                        }
                        if !ready {
                            tokio::select! { _ = &mut changed => {}, _ = self.outbound.closed() => return }
                        }
                    }
                    Err(source) => {
                        warn!(?source, "invalid UDP return storage layout");
                        return;
                    }
                }
            };
            for packet in fragments {
                if self
                    .outbound
                    .send_packet(crate::packet_output::ObservedPacket {
                        packet,
                        observer: self.outbound.observer(),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            return;
        }
        let pkt = packet::build_udp(src_ip, dst_ip, src.port, dst.port, data);
        let identification = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        let fragments = packet::fragment_ip_packet(&pkt, self.mtu, identification);
        if fragments.is_empty() {
            warn!(
                packet_bytes = pkt.len(),
                mtu = self.mtu,
                "unable to fragment UDP packet"
            );
            return;
        }
        for fragment in fragments {
            if self.outbound.send(fragment).await.is_err() {
                warn!("UDP outbound packet transport closed");
                return;
            }
        }
    }
}
