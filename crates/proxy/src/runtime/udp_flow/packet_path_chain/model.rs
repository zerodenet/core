use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use zero_core::Address;

use super::bridge::Waiter;
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
    pub(crate) build_operation: Box<dyn PreparedUdpPacketPathOperation>,
}

impl PacketPathCarrierRequest {
    pub(crate) fn upstream(&self) -> (String, u16) {
        (self.descriptor.server.clone(), self.descriptor.port)
    }
}

pub(super) struct Entry {
    pub(super) path: Arc<dyn PacketPathCarrier>,
    pub(super) waiters: Arc<Mutex<VecDeque<Waiter>>>,
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
