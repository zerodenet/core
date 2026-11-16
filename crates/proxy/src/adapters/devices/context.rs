use std::{collections::HashMap, sync::Arc};

use crate::component::stream_bridge::datagram::packet_path::RolePacketPath;

/// Publication facts for the already-hosted raw-IP devices. Inventory selects
/// the accepted packet recipes; this context carries no Engine, connector,
/// protocol registry or alternate device executor.
#[derive(Clone)]
pub(crate) struct OutboundDevicePreparationContext {
    pub(crate) direct_packet_device: Option<zero_config::DirectPacketDeviceConfig>,
    pub(crate) mtu: u16,
    pub(crate) stack_options: crate::runtime::raw_ip::RawIpStackOptions,
    pub(crate) network_generation: zero_core::component::NetworkGeneration,
    pub(crate) packet_paths: Arc<HashMap<String, PreparedDevicePacketPath>>,
}

/// One same-candidate standard carrier recipe and its exact physical-root
/// requirements. This is configuration metadata, never a resource grant.
#[derive(Clone)]
pub(crate) struct PreparedDevicePacketPath {
    pub(crate) recipe: Arc<RolePacketPath>,
    pub(crate) identity: [u8; 32],
}
