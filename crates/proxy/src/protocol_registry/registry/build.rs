use std::sync::Arc;

use super::ProtocolRegistry;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability};

type OutboundLeafClaimFn<T> =
    for<'a> fn(&T, OutboundLeafInput<'a>) -> Option<OutboundLeafClaim<'a>>;

struct ProjectedOutboundClaimer<T> {
    adapter: Arc<T>,
    claim: OutboundLeafClaimFn<T>,
}

impl<T> super::OutboundLeafClaimer for ProjectedOutboundClaimer<T>
where
    T: Send + Sync + 'static,
{
    fn claim_outbound_leaf<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        (self.claim)(self.adapter.as_ref(), input)
    }
}

impl ProtocolRegistry {
    pub(crate) fn register_standard_protocol<T>(
        &mut self,
        adapter: Arc<T>,
        claim: Option<OutboundLeafClaimFn<T>>,
    ) where
        T: ProtocolSupportCapability + 'static,
    {
        assert!(
            self.inbounds.has_factory(adapter.name()),
            "standard inbound requires its actual component factory"
        );
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            outbound: claim.map(|claim| {
                Arc::new(ProjectedOutboundClaimer {
                    adapter: adapter.clone(),
                    claim,
                }) as Arc<dyn super::OutboundLeafClaimer>
            }),
        });
    }
    #[cfg(test)]
    pub(crate) fn without_outbound_projection(mut self, protocol: &str) -> Self {
        self.entries
            .iter_mut()
            .find(|entry| entry.support.name() == protocol)
            .expect("real compiled protocol")
            .outbound = None;
        self
    }
    // Endpoint hooks are kept neutral even in builds without a registered
    // endpoint protocol. WireGuard is the current opt-in implementation.
    #[allow(dead_code, reason = "some builds register no endpoint controller")]
    pub(crate) fn register_endpoint_controller<
        T: crate::protocol_registry::EndpointControlCapability + 'static,
    >(
        &mut self,
        adapter: Arc<T>,
    ) {
        self.endpoint_controllers.push(adapter);
    }

    #[allow(dead_code, reason = "some builds register no endpoint observer")]
    pub(crate) fn register_endpoint_observer<
        T: crate::protocol_registry::EndpointObservationCapability + 'static,
    >(
        &mut self,
        adapter: Arc<T>,
    ) {
        self.endpoint_observers.push(adapter);
    }

    fn push_registered_entry(&mut self, entry: super::RegisteredProtocolEntry) {
        let name = entry.support.name();
        assert!(
            !self
                .entries
                .iter()
                .any(|registered| registered.support.name() == name),
            "protocol capability `{name}` registered more than once"
        );
        self.entries.push(entry);
    }
}
