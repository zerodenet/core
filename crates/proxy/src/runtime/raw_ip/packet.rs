//! Generic raw-IP packet route backed by a protocol-owned tunnel plan.

use std::{io, sync::Arc};

use async_trait::async_trait;
use tokio::sync::mpsc;
use zero_stack::packet;

use super::{RawIpDevicePool, RawIpOutboundPlan};
use crate::runtime::packet_route::{PacketForwardObservation, PreparedPacketRouteOperation};

pub(crate) struct RawIpPacketOperation {
    pub(crate) translate_source: bool,
    pub(crate) tag: String,
    pub(crate) identity: [u8; 32],
    pub(crate) plan: Arc<dyn RawIpOutboundPlan>,
    pub(crate) pool: Arc<RawIpDevicePool>,
}

#[async_trait]
impl PreparedPacketRouteOperation for RawIpPacketOperation {
    async fn forward(
        &self,
        mut packet: Vec<u8>,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
        egress_generation: u64,
    ) -> io::Result<PacketForwardObservation> {
        let source = packet::ip_source(&packet).ok_or_else(invalid_packet)?;
        let destination = packet::ip_destination(&packet).ok_or_else(invalid_packet)?;
        let mtu = usize::from(self.plan.mtu());
        let requires_fragmentation = packet.len() > mtu;
        let ipv4_may_fragment = packet::ipv4_fragmentation_allowed(&packet);
        if requires_fragmentation && !ipv4_may_fragment {
            return Ok(PacketForwardObservation::local(
                packet::build_icmp_response(&packet, mtu),
            ));
        }
        if !self.translate_source && self.plan.is_local_address(source) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "forwarded source overlaps tunnel local address",
            ));
        }
        let peer = self
            .plan
            .peer_for_target(destination)
            .map_err(io::Error::other)?;
        let cell = self
            .pool
            .cell_for(&self.tag, peer.peer_index, self.identity, egress_generation)
            .map_err(io::Error::other)?;
        let device = cell
            .get()
            .ok_or_else(|| io::Error::other("raw-IP peer device not initialized"))?;
        if !self.pool.is_current(
            &self.tag,
            peer.peer_index,
            self.identity,
            egress_generation,
            device,
        ) {
            return Err(io::Error::other("raw-IP peer device changed"));
        }
        if packet::ip_hop_limit(&packet).is_some_and(|limit| limit <= 1) {
            return Ok(PacketForwardObservation::local(
                packet::build_icmp_time_exceeded_response(
                    &packet,
                    peer.local_ip,
                    usize::from(self.plan.mtu()),
                ),
            ));
        }
        if !packet::advance_ip_hop(&mut packet) {
            return Err(invalid_packet());
        }
        let peer_identity = self.plan.peer_identity(peer.peer_index);
        if self.translate_source {
            return device
                .forward_translated_packet(&packet, peer.local_ip, replies, mtu)
                .map(|()| PacketForwardObservation::forwarded(peer_identity));
        }
        let packets = if requires_fragmentation {
            let fragments = packet::fragment_forwarded_packet(&packet, mtu);
            if fragments.is_empty() {
                return Err(invalid_packet());
            }
            fragments
        } else {
            vec![packet]
        };
        device.forward_packets(packets, source, ingress_id, replies)?;
        Ok(PacketForwardObservation::forwarded(peer_identity))
    }
}

fn invalid_packet() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid IP packet")
}
