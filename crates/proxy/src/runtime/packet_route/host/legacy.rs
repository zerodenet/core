//! Remaining explicit PacketBuffer library bridge; ordinary Sources use owned roles.
use super::*;
#[async_trait::async_trait]
impl PreparedPacketRouteOperation for Arc<HostPacketDevice> {
    async fn forward(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        _: u64,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        if self.native_provider.get().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "hosted native Packet requires its standard role and owned submission",
            ));
        }
        let hop = packet::ip_hop_limit(packet);
        let result = self
            .forward_inner(packet, ingress_id, replies, observer)
            .await;
        if result.is_err() && packet::ip_hop_limit(packet) != hop {
            if let Some(hop) = hop {
                packet::restore_ip_hop(packet, hop);
            }
        }
        result
    }
}
impl HostPacketDevice {
    async fn forward_inner(
        &self,
        packet: &mut zero_traits::PacketBuffer,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> io::Result<PacketForwardObservation> {
        if !self.usable() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "host packet device stopped",
            ));
        }
        let source = packet::ip_source(packet)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid IP packet"))?;
        let router = self
            .config
            .router_addresses
            .iter()
            .find(|ip| ip.is_ipv4() == source.is_ipv4());
        if packet.len() > self.mtu && !packet::ipv4_fragmentation_allowed(packet) {
            return Ok(PacketForwardObservation::local(router.and_then(|router| {
                packet::build_icmp_mtu_error_response(packet, *router, self.mtu)
            })));
        }
        let conversation = packet::packet_conversation_key(packet);
        if packet::ip_hop_limit(packet).is_some_and(|limit| limit <= 1) {
            return Ok(PacketForwardObservation::local(router.and_then(|router| {
                packet::build_icmp_time_exceeded_response(packet, *router, self.mtu)
            })));
        }
        if !packet::advance_ip_hop(packet) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid IP packet",
            ));
        }
        let fragmented = packet.len() > self.mtu;
        let mut fragments = if fragmented {
            packet::fragment_forwarded_packet(packet, self.mtu)
                .into_iter()
                .map(Into::into)
                .collect::<Vec<zero_traits::PacketBuffer>>()
        } else {
            Vec::new()
        };
        if fragmented && fragments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet cannot be fragmented",
            ));
        }
        let return_channel = replies.clone();
        self.returns.register_observed(
            source,
            ingress_id,
            replies,
            observer.clone(),
            conversation,
        )?;
        if !fragmented {
            fragments.push(std::mem::take(packet));
        }
        for outgoing in fragments.drain(..) {
            let (ack, done) = oneshot::channel();
            let permit = match self.writes.try_reserve() {
                Ok(permit) => permit,
                Err(_) => {
                    if !fragmented {
                        *packet = outgoing;
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "host packet queue full",
                    ));
                }
            };
            permit.send(Write::Legacy(LegacyWrite {
                return_channel: return_channel.clone(),
                packet: outgoing,
                observer: observer.clone(),
                ack,
            }));
            let (outgoing, result) = done.await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "host packet writer stopped")
            })?;
            if !fragmented {
                *packet = outgoing;
            }
            result?;
        }
        Ok(PacketForwardObservation::forwarded(None))
    }
}
