use std::sync::Arc;

use zero_core::Address;

use super::bridge::Waiters;
use super::key::PathKey;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;
use crate::runtime::udp_flow::packet_path::{
    DatagramCodec, PacketPathCarrierDescriptor, UdpDatagramEndpoint, UdpDatagramSource,
    UdpPacketRef,
};

pub(crate) struct PacketPathStartRequest<'a> {
    pub(crate) session_id: u64,
    pub(crate) carrier: PacketPathCarrierRequest,
    pub(crate) datagram: UdpDatagramSource,
    pub(crate) packet: UdpPacketRef<'a>,
}

pub(crate) struct PacketPathCarrierRequest {
    pub(crate) descriptor: PacketPathCarrierDescriptor,
    pub(crate) build_operation: Arc<dyn PreparedUdpPacketPathOperation>,
}

impl PacketPathCarrierRequest {
    pub(crate) fn upstream(&self) -> (String, u16) {
        (self.descriptor.server.clone(), self.descriptor.port)
    }
}

pub(super) struct Entry {
    pub(super) path: Option<Arc<dyn PacketPathCarrier>>,
    pub(super) retiring: Option<crate::component::datagram_retirement::DatagramRegistration>,
    pub(super) receiving: super::receiver::PathStatus,
    pub(super) waiters: zero_traits::packet_metadata::PacketMetadataArc<Waiters>,
    pub(super) memory: crate::protocol_registry::packet_memory::business::BusinessMemory,
    pub(super) codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    pub(super) datagram_endpoint: UdpDatagramEndpoint,
}

pub(super) struct EntryCandidate {
    pub(super) carrier_desc: PacketPathCarrierDescriptor,
    pub(super) datagram: UdpDatagramSource,
}

impl EntryCandidate {
    pub(super) fn key(&self) -> PathKey {
        PathKey::from_sources(&self.carrier_desc, self.datagram.descriptor().key_part())
    }
}

/// This same cache's concrete key/endpoint strings and table are prepaid before
/// opening a path. Settled rows are removed only after actual control closure.
pub(super) struct CacheRow {
    pub(super) key: PathKey,
    pub(super) entry: Entry,
    pub(super) _strings: zero_traits::packet_metadata::PacketMetadataCredit,
}
