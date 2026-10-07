//! Protocol-neutral shared raw-IP device and peer cache.

mod carrier;
mod contract;
mod datagram;
mod device;
mod incarnation;
mod packet;
mod pool;
pub(crate) use incarnation::next as next_incarnation;

pub(crate) use carrier::{DirectRawIpWireCarrier, ProxiedRawIpWireCarrier, RawIpWireCarrier};
pub(crate) use contract::{RawIpAction, RawIpOutboundPlan, RawIpPeerPlan, RawIpTunnel};
pub(crate) use datagram::RawIpDatagramOperation;
pub(crate) use device::PacketReturns;
pub(crate) use device::{EndpointPacket, SharedRawIpDevice};
pub(crate) use packet::RawIpPacketOperation;
pub(crate) use pool::{RawIpDevicePool, StagedRawIpDevices, MAX_RAW_IP_DEVICES};

mod statistics;
pub(crate) use statistics::RawIpTraffic;
