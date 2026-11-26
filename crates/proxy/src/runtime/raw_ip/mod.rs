//! Protocol-neutral shared raw-IP device and peer cache.

#[cfg(feature = "peer-raw-ip-runtime")]
mod carrier;
#[cfg(feature = "peer-raw-ip-runtime")]
mod contract;
#[cfg(feature = "peer-raw-ip-runtime")]
mod device;
#[cfg(not(feature = "peer-raw-ip-runtime"))]
mod device {
    pub(super) mod returns;
}
#[cfg(all(test, feature = "peer-raw-ip-runtime"))]
mod flow;
#[cfg(feature = "peer-raw-ip-runtime")]
mod incarnation;
pub(crate) mod memory;
#[cfg(feature = "peer-raw-ip-runtime")]
mod pool;
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use incarnation::next as next_incarnation;

#[cfg(all(test, feature = "peer-raw-ip-runtime"))]
pub(crate) use carrier::DirectRawIpWireCarrier;
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use carrier::RegisteredProxiedRawIpWireCarrier;
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use carrier::{HostedRawIpWireCarrier, RawIpWireCarrier};
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use contract::{
    OwnedWireFailure, OwnedWireResult, PreparedWireData, PreparedWireReceive, RawIpAction,
    RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel,
};
#[cfg(not(feature = "peer-raw-ip-runtime"))]
pub(crate) use device::returns::{PacketReturnTotals, PacketReturns};
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use device::{
    EndpointPacket, EndpointStackOwner, EndpointStackRecipe, PendingNativeWire, SharedRawIpDevice,
    WireStackRecipe,
};
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use device::{
    PacketFlowBorrow, PacketReturnTotals, PacketReturns, PacketSubmission, ReturnSource,
};
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) use pool::{RawIpDevicePool, StagedRawIpDevices, MAX_RAW_IP_DEVICES};

mod options;
pub(crate) use options::RawIpStackOptions;
mod statistics;
pub(crate) use statistics::RawIpTraffic;
#[cfg(feature = "peer-raw-ip-runtime")]
pub(crate) mod timer;

pub(crate) mod tasks;

#[cfg(all(test, feature = "peer-raw-ip-runtime"))]
#[path = "tests/memory.rs"]
pub(crate) mod test_memory;
