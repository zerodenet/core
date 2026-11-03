use std::fmt;
use std::sync::Arc;

use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability};

mod build;
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
    pub(crate) inbounds: crate::component::ComponentRegistry<zero_config::InboundConfig>,
    entries: Vec<RegisteredProtocolEntry>,
    endpoint_observers: Vec<Arc<dyn crate::protocol_registry::EndpointObservationCapability>>,
    endpoint_controllers: Vec<Arc<dyn crate::protocol_registry::EndpointControlCapability>>,
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
    outbound: Option<Arc<dyn OutboundLeafClaimer>>,
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
