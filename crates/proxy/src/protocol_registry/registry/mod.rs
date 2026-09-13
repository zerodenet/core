use std::fmt;
use std::sync::Arc;

#[cfg(feature = "managed-udp-runtime")]
use crate::protocol_registry::ManagedUdpHandlerProvider;
#[cfg(test)]
use crate::protocol_registry::TcpOutboundCapability;
#[cfg(feature = "upstream-association-runtime")]
use crate::protocol_registry::UpstreamUdpHandlerProvider;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability,
};
#[cfg(all(test, feature = "udp-runtime"))]
use crate::protocol_registry::{UdpFlowCapability, UdpPacketPathCapability};

mod build;
mod inbound;
mod metadata;
mod outbound;
mod runtime;
#[cfg(feature = "managed-stream-runtime")]
mod service;
mod support;
mod validation;

pub(crate) use outbound::ClaimedOutboundLeaf;

/// Registry of all compiled-in protocol adapters.
///
/// Constructed only by `register.rs`. Replaces manual protocol matches in
/// inventory and generic runtime code.
#[derive(Clone, Default)]
pub(crate) struct ProtocolRegistry {
    entries: Vec<RegisteredProtocolEntry>,
    #[cfg(feature = "managed-stream-runtime")]
    services: Vec<Arc<dyn crate::protocol_registry::InboundServiceCapability>>,
}

pub(crate) trait OutboundLeafClaimer: Send + Sync {
    fn claim_outbound_leaf<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>>;
}

#[derive(Clone)]
struct RegisteredProtocolEntry {
    support: Arc<dyn ProtocolSupportCapability>,
    inbound: Arc<dyn InboundListenerCapability>,
    outbound: Arc<dyn OutboundLeafClaimer>,
    #[cfg(test)]
    tcp: Arc<dyn TcpOutboundCapability>,
    #[cfg(all(test, feature = "udp-runtime"))]
    udp: Option<Arc<dyn UdpFlowCapability>>,
    #[cfg(feature = "managed-udp-runtime")]
    managed_udp_handlers: Option<Arc<dyn ManagedUdpHandlerProvider>>,
    #[cfg(feature = "upstream-association-runtime")]
    upstream_udp_handler: Option<Arc<dyn UpstreamUdpHandlerProvider>>,
    #[cfg(all(test, feature = "udp-runtime"))]
    packet_path: Option<Arc<dyn UdpPacketPathCapability>>,
}

impl fmt::Debug for ProtocolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolRegistry")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests;
