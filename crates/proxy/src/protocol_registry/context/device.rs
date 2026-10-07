use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{PacketPathExecutionServices, UpstreamConnectServices};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;

/// Prepared dependencies for outbound raw-IP devices. Packet-path operations
/// are claimed at the inventory boundary, so a device adapter never parses another
/// protocol's configuration or retains the registry itself.
#[derive(Clone)]
pub(crate) struct OutboundDevicePreparationContext {
    pub(crate) traffic: zero_engine::Engine,
    pub(crate) direct_packet_device: Option<zero_config::DirectPacketDeviceConfig>,
    pub(crate) mtu: u16,
    pub(crate) endpoint_bindings: Arc<Vec<zero_config::EndpointBindingConfig>>,
    /// Business role admission is separate from physical device lifetime.
    pub(crate) disabled_outbounds: Arc<HashSet<String>>,
    pub(crate) upstream: UpstreamConnectServices,
    pub(crate) packet_path_services: PacketPathExecutionServices,
    pub(crate) packet_paths: Arc<HashMap<String, Arc<dyn PreparedUdpPacketPathOperation>>>,
    pub(crate) packet_path_identities: Arc<HashMap<String, [u8; 32]>>,
}
