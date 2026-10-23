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
    pub(crate) stack_options: crate::runtime::raw_ip::RawIpStackOptions,
    pub(crate) endpoint_bindings: Arc<Vec<zero_config::EndpointBindingConfig>>,
    /// Business role admission is separate from physical device lifetime.
    pub(crate) disabled_outbounds: Arc<HashSet<String>>,
    pub(crate) upstream: UpstreamConnectServices,
    pub(crate) packet_path_services: PacketPathExecutionServices,
    pub(crate) packet_paths: Arc<HashMap<String, PreparedDevicePacketPath>>,
}

/// One inventory-claimed static carrier recipe and its exact physical-root
/// requirements. This is configuration metadata, never a resource grant.
#[derive(Clone)]
pub(crate) struct PreparedDevicePacketPath {
    pub(crate) operation: Arc<dyn PreparedUdpPacketPathOperation>,
    pub(crate) identity: [u8; 32],
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(crate) root_binding: zero_platform_tokio::contract::network::UpstreamBinding,
}
