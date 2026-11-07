//! Protocol-neutral shared raw-IP device and peer cache.

mod carrier;
mod contract;
mod datagram;
mod flow;
pub(crate) use flow::RawIpFlowSource;
mod device;
mod incarnation;
pub(crate) mod memory;
mod packet;
mod pool;
pub(crate) use incarnation::next as next_incarnation;

#[cfg(test)]
pub(crate) use carrier::DirectRawIpWireCarrier;
pub(crate) use carrier::{HostedRawIpWireCarrier, ProxiedRawIpWireCarrier, RawIpWireCarrier};
pub(crate) use contract::{
    OwnedWireFailure, OwnedWireResult, RawIpAction, RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel,
};
pub(crate) use datagram::RawIpDatagramOperation;
pub(crate) use device::{
    EndpointPacket, EndpointStackOwner, EndpointStackRecipe, SharedRawIpDevice, WireStackRecipe,
};
pub(crate) use device::{
    PacketFlowBorrow, PacketReturnTotals, PacketReturns, PacketSubmission, ReturnSource,
};
pub(crate) use packet::RawIpPacketOperation;
pub(crate) use pool::{RawIpDevicePool, StagedRawIpDevices, MAX_RAW_IP_DEVICES};

mod options;
pub(crate) use options::RawIpStackOptions;
mod statistics;
pub(crate) use statistics::RawIpTraffic;
pub(crate) mod timer;

pub(crate) mod tasks;
