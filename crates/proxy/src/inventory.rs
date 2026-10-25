use crate::protocol_registry::ProtocolRegistry;

mod component;
#[cfg(feature = "raw-ip-runtime")]
mod device;
mod dns_dependencies;
mod endpoint;
mod inbound;
mod metadata;
#[cfg(feature = "raw-ip-runtime")]
mod packet;
mod protocols;
mod runtime;
pub(crate) mod service;
mod tcp;
#[cfg(feature = "udp-runtime")]
mod udp;
mod weak;

#[cfg(feature = "raw-ip-runtime")]
pub(crate) use packet::PacketRouteTarget;
pub(crate) use runtime::{ClaimedInventoryLeaf, ClaimedRelayChain};
pub(crate) use tcp::PreparedNativeTcpRelayChain;
pub(crate) use tcp::{
    PreparedTcpCandidate, PreparedTcpCandidateExecution, PreparedTcpOutbound, PreparedTcpRelayHop,
};
pub(crate) use tcp::{PreparedTcpRelayChain, PreparedTcpRelayPrefix};
#[cfg(feature = "udp-runtime")]
pub(crate) use udp::{PreparedUdpLeafCandidate, PreparedUdpOutbound};
pub(crate) use weak::WeakProtocolInventory;

#[derive(Debug, Clone)]
pub struct ProtocolInventory {
    registry: std::sync::Arc<ProtocolRegistry>,
    host: std::sync::Arc<crate::runtime::RuntimeHost>,
}

impl Default for ProtocolInventory {
    fn default() -> Self {
        let (registry, factories) = crate::register::compiled_registries();
        Self {
            registry: std::sync::Arc::new(registry),
            host: std::sync::Arc::new(crate::runtime::RuntimeHost::new(factories)),
        }
    }
}

impl ProtocolInventory {
    pub(crate) fn with_background(
        network: std::sync::Arc<crate::component::background::BackgroundNetwork>,
    ) -> Self {
        let (registry, factories) =
            crate::register::compiled_registries_with_background(Some(network));
        Self {
            registry: std::sync::Arc::new(registry),
            host: std::sync::Arc::new(crate::runtime::RuntimeHost::new(factories)),
        }
    }
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn registered_udp_handlers(
        &self,
    ) -> crate::runtime::udp_flow::registered::RegisteredUdpHandlers {
        crate::register::registered_udp_handlers(&self.registry)
    }
}

impl ProtocolInventory {
    pub(crate) fn runtime_host(&self) -> std::sync::Arc<crate::runtime::RuntimeHost> {
        self.host.clone()
    }
}
