use zero_config::RuntimeConfig;

#[cfg(feature = "udp-runtime")]
use super::{PacketPathExecutionServices, UdpRuntimeServices};

#[derive(Clone, Copy)]
pub(crate) struct OutboundAdapterContext<'a> {
    config: &'a RuntimeConfig,
}

impl<'a> OutboundAdapterContext<'a> {
    pub(crate) fn new(config: &'a RuntimeConfig) -> Self {
        Self { config }
    }

    pub(crate) fn source_dir(&self) -> Option<&std::path::Path> {
        self.config.source_dir()
    }

    pub(crate) fn config(&self) -> &'a RuntimeConfig {
        self.config
    }
}

#[derive(Clone)]
#[cfg(feature = "udp-runtime")]
pub(crate) struct UdpAdapterContext<'a> {
    config: &'a RuntimeConfig,
    services: UdpRuntimeServices,
}

#[cfg(feature = "udp-runtime")]
impl<'a> UdpAdapterContext<'a> {
    pub(crate) fn new(config: &'a RuntimeConfig, services: UdpRuntimeServices) -> Self {
        Self { config, services }
    }

    pub(crate) fn source_dir(&self) -> Option<&'a std::path::Path> {
        self.config.source_dir()
    }

    pub(crate) fn config(&self) -> &'a RuntimeConfig {
        self.config
    }

    pub(crate) fn udp_enabled_for_outbound(&self, outbound_tag: Option<&str>) -> bool {
        self.services.udp_enabled_for_outbound(outbound_tag)
    }

    pub(crate) fn runtime_services(&self) -> UdpRuntimeServices {
        self.services.clone()
    }

    pub(crate) fn packet_path_execution_services(&self) -> PacketPathExecutionServices {
        self.services.packet_path_execution()
    }
}
