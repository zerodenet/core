use std::fmt;
use std::sync::Arc;

use crate::protocol_registry::ProtocolSupportCapability;

mod build;
mod metadata;
#[cfg(feature = "udp-runtime")]
mod outbound;
mod runtime;
#[cfg(feature = "managed-stream-runtime")]
mod service;
mod support;
mod validation;

#[cfg(feature = "udp-runtime")]
pub(crate) use outbound::ColdPacketPathProjection;

/// Registry of all compiled-in protocol adapters.
///
/// Constructed only by `register.rs`. Replaces manual protocol matches in
/// inventory and generic runtime code.
#[derive(Clone, Default)]
pub(crate) struct ProtocolRegistry {
    pub(crate) inbounds: crate::component::ComponentRegistry<zero_config::InboundConfig>,
    entries: Vec<RegisteredProtocolEntry>,
    endpoint_observers: Vec<Arc<dyn crate::protocol_registry::EndpointObservationCapability>>,
    endpoint_controllers: Vec<Arc<dyn crate::protocol_registry::EndpointControlCapability>>,
    #[cfg(feature = "managed-stream-runtime")]
    services: Vec<Arc<dyn crate::protocol_registry::InboundServiceCapability>>,
}

#[derive(Clone)]
struct RegisteredProtocolEntry {
    support: Arc<dyn ProtocolSupportCapability>,
    // Cold material declaration only. It grants no IO, native, Scope or route.
    #[cfg(feature = "udp-runtime")]
    packet_projection: Option<Arc<PacketPathProjection>>,
}

#[cfg(feature = "udp-runtime")]
type PacketPathProjection = dyn Fn(
        &zero_config::OutboundConfig,
    ) -> Result<Option<ColdPacketPathProjection>, zero_engine::EngineError>
    + Send
    + Sync;

impl fmt::Debug for ProtocolRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolRegistry")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

#[cfg(test)]
mod tests;
