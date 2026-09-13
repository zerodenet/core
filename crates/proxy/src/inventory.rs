use crate::protocol_registry::ProtocolRegistry;

mod inbound;
mod metadata;
mod protocols;
mod runtime;
mod tcp;
#[cfg(feature = "udp-runtime")]
mod udp;
mod weak;

pub(crate) use runtime::{ClaimedInventoryLeaf, ClaimedRelayChain};
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
}

impl Default for ProtocolInventory {
    fn default() -> Self {
        Self {
            registry: std::sync::Arc::new(crate::register::protocol_registry()),
        }
    }
}

#[cfg(feature = "udp-runtime")]
impl ProtocolInventory {
    pub(crate) fn registered_udp_handlers(
        &self,
    ) -> crate::runtime::udp_flow::registered::RegisteredUdpHandlers {
        crate::register::registered_udp_handlers(&self.registry)
    }
}
