use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::ConfigError;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Public runtime configuration schema generation.
    ///
    /// Omitted legacy configurations are V1. Unknown explicit versions fail
    /// validation before any runtime state is constructed.
    #[serde(default = "default_config_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    #[serde(default)]
    pub inbounds: Vec<InboundConfig>,
    #[serde(default)]
    pub outbounds: Vec<OutboundConfig>,
    #[serde(default)]
    pub outbound_groups: Vec<OutboundGroupConfig>,
    #[serde(default)]
    pub runtime: RuntimeOptionsConfig,
    #[serde(default)]
    pub mode: ModeConfig,
    pub route: RouteConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(skip)]
    pub source_dir: Option<PathBuf>,
}

const fn default_config_schema_version() -> u32 {
    zero_api::CONFIG_SCHEMA_VERSION
}

#[derive(Deserialize)]
struct RuntimeConfigVersionProbe {
    #[serde(default = "default_config_schema_version")]
    schema_version: u32,
}

impl RuntimeConfig {
    pub fn parse(raw: &str) -> Result<Self, ConfigError> {
        Self::parse_with_source_dir(raw, None)
    }

    pub fn source_dir(&self) -> Option<&Path> {
        self.source_dir.as_deref()
    }

    /// Parse an in-memory configuration using the supplied directory as the
    /// base for relative state, credential, and artifact paths.
    ///
    /// Control-plane configuration replacement should pass the current
    /// configuration's source directory so a hot apply has the same path
    /// semantics as process startup.
    pub fn parse_with_source_dir(
        raw: &str,
        source_dir: Option<PathBuf>,
    ) -> Result<Self, ConfigError> {
        let raw = raw.strip_prefix('\u{feff}').unwrap_or(raw);
        let version = serde_json::from_str::<RuntimeConfigVersionProbe>(raw)?.schema_version;
        if version != zero_api::CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::UnsupportedSchemaVersion {
                found: version,
                supported: zero_api::CONFIG_SCHEMA_VERSION,
            });
        }
        let mut config = serde_json::from_str::<Self>(raw)?;
        config.source_dir = source_dir;
        config.validate_endpoint_projection_collisions()?;
        config.normalize();
        config.materialize_endpoints()?;
        config.validate()?;

        Ok(config)
    }

    fn normalize(&mut self) {
        for inbound in &mut self.inbounds {
            inbound.protocol.normalize();
        }

        for outbound in &mut self.outbounds {
            outbound.protocol.normalize();
        }
    }
}

mod runtime;
pub use runtime::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkOptionsConfig {
    /// Dedicated, inherited TUN descriptor. The host owns its addresses/routes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_packet_device: Option<DirectPacketDeviceConfig>,
    /// MTU requested from the TUN backend and used by its user-space stack.
    #[serde(default = "default_network_mtu")]
    pub mtu: u16,
    /// Neutral UDP reply admission for Packet-to-Flow client stacks.
    #[serde(default)]
    pub udp_receive_queue: UdpReceiveQueueConfig,
}

impl Default for NetworkOptionsConfig {
    fn default() -> Self {
        Self {
            direct_packet_device: None,
            mtu: default_network_mtu(),
            udp_receive_queue: UdpReceiveQueueConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectPacketDeviceConfig {
    #[serde(
        default,
        skip_serializing_if = "DirectPacketDeviceBackend::is_descriptor"
    )]
    pub backend: DirectPacketDeviceBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fd: Option<i32>,
    pub interface: String,
    /// Host-owned interface addresses used as sources for local ICMP errors.
    pub router_addresses: Vec<std::net::IpAddr>,
    /// Accepted writes retained by this actual host Packet device's FIFO.
    /// On demand, 128 by default; changing this value replaces the old device.
    #[serde(
        default = "default_direct_packet_write_queue",
        skip_serializing_if = "is_default_direct_packet_write_queue"
    )]
    pub write_queue: std::num::NonZeroUsize,
}

fn default_direct_packet_write_queue() -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(128).unwrap()
}
fn is_default_direct_packet_write_queue(value: &std::num::NonZeroUsize) -> bool {
    *value == default_direct_packet_write_queue()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectPacketDeviceBackend {
    #[default]
    Descriptor,
    Wintun,
}

impl DirectPacketDeviceBackend {
    fn is_descriptor(&self) -> bool {
        *self == Self::Descriptor
    }
}

const fn default_network_mtu() -> u16 {
    1500
}

const fn default_udp_upstream_idle_timeout_seconds() -> u64 {
    30
}

const fn default_event_log_capacity() -> usize {
    DEFAULT_EVENT_LOG_CAPACITY
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UdpPolicyConfig {
    #[serde(default = "default_udp_enabled")]
    pub enabled: bool,
}

impl Default for UdpPolicyConfig {
    fn default() -> Self {
        Self {
            enabled: default_udp_enabled(),
        }
    }
}

const fn default_udp_enabled() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ModeConfig {
    #[default]
    #[serde(rename = "rule")]
    Rule,
    #[serde(rename = "global")]
    Global { outbound: String },
    #[serde(rename = "direct")]
    Direct,
}

impl ModeConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Rule => "rule",
            Self::Global { .. } => "global",
            Self::Direct => "direct",
        }
    }

    pub fn outbound(&self) -> Option<&str> {
        match self {
            Self::Global { outbound } => Some(outbound),
            Self::Rule | Self::Direct => None,
        }
    }
}

mod api;
mod dial;
mod dns;
mod endpoint;
mod inbound;
mod log;
mod mkcp;
mod outbound;
mod route;
mod serialization;
mod transport;
mod wireguard;
pub use mkcp::MkcpConfig;
mod udp_queue;
pub use udp_queue::UdpReceiveQueueConfig;
mod tun;

pub use api::*;
pub use dial::*;
pub use dns::*;
pub use endpoint::*;
pub use inbound::*;
pub use log::*;
pub use outbound::*;
pub use route::*;
pub use transport::*;
pub use tun::*;
pub use wireguard::*;

mod hysteria2;
pub use hysteria2::*;

mod hysteria_carrier;
pub use hysteria_carrier::{
    HysteriaCarrierMasqueradeConfig, HysteriaTransportConfig, QuicParametersConfig, UdpHopConfig,
};

mod finalmask;
pub use finalmask::{
    FinalMaskConfig, MaskItemConfig, MaskRangeConfig, NoiseItemConfig, TcpMaskConfig,
    TcpMaskItemConfig, UdpMaskConfig,
};

pub use transport::{
    ClientTlsOptionsConfig, EchForceQueryConfig, ServerTlsOptionsConfig, TlsBackendConfig,
    TlsCertificateFilesConfig, TlsCertificateUsageConfig, TlsParametersConfig,
};
