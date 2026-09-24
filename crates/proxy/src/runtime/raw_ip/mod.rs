//! Protocol-neutral shared raw-IP device and peer cache.

mod contract;
mod datagram;
mod device;
mod packet;
mod pool;

pub(crate) use contract::{RawIpAction, RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel};
pub(crate) use datagram::RawIpDatagramOperation;
pub(crate) use device::SharedRawIpDevice;
pub(crate) use packet::RawIpPacketOperation;
pub(crate) use pool::{RawIpDevicePool, StagedRawIpDevices, MAX_RAW_IP_DEVICES};
