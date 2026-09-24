//! Generic raw-IP packet route backed by a protocol-owned tunnel plan.

use std::{io, sync::Arc};

use async_trait::async_trait;
use tokio::sync::mpsc;
use zero_stack::packet;

use super::{RawIpDevicePool, RawIpOutboundPlan};
use crate::runtime::packet_route::PreparedPacketRouteOperation;

pub(crate) struct RawIpPacketOperation {
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
    ) -> io::Result<Option<Vec<u8>>> {
        let source = packet::ip_source(&packet).ok_or_else(invalid_packet)?;
        let destination = packet::ip_destination(&packet).ok_or_else(invalid_packet)?;
        let mtu = usize::from(self.plan.mtu());
        let requires_fragmentation = packet.len() > mtu;
        let ipv4_may_fragment = packet.first().is_some_and(|version| version >> 4 == 4)
            && packet.len() >= 8
            && u16::from_be_bytes([packet[6], packet[7]]) & 0x4000 == 0;
        if requires_fragmentation && !ipv4_may_fragment {
            return Ok(packet::build_icmp_response(&packet, mtu));
        }
        if self.plan.is_local_address(source) {
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
            return Ok(packet::build_icmp_time_exceeded_response(
                &packet,
                peer.local_ip,
                usize::from(self.plan.mtu()),
            ));
        }
        if !packet::advance_ip_hop(&mut packet) {
            return Err(invalid_packet());
        }
        let packets = if requires_fragmentation {
            let identification = u32::from(u16::from_be_bytes([packet[4], packet[5]]));
            let fragments = packet::fragment_ip_packet(&packet, mtu, identification);
            if fragments.is_empty() {
                return Err(invalid_packet());
            }
            fragments
        } else {
            vec![packet]
        };
        device.forward_packets(packets, source, ingress_id, replies)?;
        Ok(None)
    }
}

fn invalid_packet() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid IP packet")
}
