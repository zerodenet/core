//! Packet-plane claim for the shared WireGuard peer device.

use std::sync::Arc;

use crate::{
    protocol_registry::ClaimedPacketLeaf,
    runtime::{
        packet_route::{PreparedDatagramExchangeOperation, PreparedPacketRouteOperation},
        raw_ip::{RawIpDatagramOperation, RawIpDevicePool, RawIpPacketOperation},
        tcp_dispatch::operation::{PreparedTcpConnectOperation, RawIpTcpOperation},
        udp_dispatch::operation::PreparedUdpFlowOperation,
        udp_flow::managed::raw_ip::RawIpUdpOperation,
    },
};

use super::udp::WireguardRawIpPlan;

pub(super) struct WireguardPacketLeaf {
    pub(super) tag: String,
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) identity: [u8; 32],
    pub(super) pool: Arc<RawIpDevicePool>,
}

impl ClaimedPacketLeaf for WireguardPacketLeaf {
    fn prepare_packet_route(&self) -> Box<dyn PreparedPacketRouteOperation> {
        Box::new(RawIpPacketOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        })
    }

    fn prepare_datagram_exchange(&self) -> Option<Box<dyn PreparedDatagramExchangeOperation>> {
        Some(Box::new(RawIpDatagramOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        }))
    }

    fn prepare_tcp_flow(&self) -> Option<Box<dyn PreparedTcpConnectOperation>> {
        Some(Box::new(RawIpTcpOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        }))
    }

    fn prepare_udp_flow(&self) -> Option<Box<dyn PreparedUdpFlowOperation>> {
        Some(Box::new(RawIpUdpOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        }))
    }
}
