//! Protocol-neutral shared raw-IP device and peer cache.

mod carrier;
mod contract;
mod device;
#[cfg(test)]
mod flow;
mod incarnation;
pub(crate) mod memory;
mod pool;
pub(crate) use incarnation::next as next_incarnation;

#[cfg(test)]
pub(crate) use carrier::DirectRawIpWireCarrier;
pub(crate) use carrier::RegisteredProxiedRawIpWireCarrier;
pub(crate) use carrier::{HostedRawIpWireCarrier, RawIpWireCarrier};
pub(crate) use contract::{
    OwnedWireFailure, OwnedWireResult, PreparedWireData, PreparedWireReceive, RawIpAction,
    RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel,
};
pub(crate) use device::{
    EndpointPacket, EndpointStackOwner, EndpointStackRecipe, PendingNativeWire, SharedRawIpDevice,
    WireStackRecipe,
};
pub(crate) use device::{
    PacketFlowBorrow, PacketReturnTotals, PacketReturns, PacketSubmission, ReturnSource,
};
pub(crate) use pool::{RawIpDevicePool, StagedRawIpDevices, MAX_RAW_IP_DEVICES};

mod options;
pub(crate) use options::RawIpStackOptions;
mod statistics;
pub(crate) use statistics::RawIpTraffic;
pub(crate) mod timer;

pub(crate) mod tasks;
