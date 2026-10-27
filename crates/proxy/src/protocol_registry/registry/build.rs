use std::sync::Arc;

use super::ProtocolRegistry;
#[cfg(feature = "managed-udp-runtime")]
use crate::protocol_registry::ManagedUdpHandlerProvider;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability,
};

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

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn register_outbound_device_lifecycle<T>(&mut self, adapter: Arc<T>)
    where
        T: crate::protocol_registry::OutboundDeviceLifecycleCapability + 'static,
    {
        self.outbound_devices.push(adapter);
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

    pub(crate) fn register_core_capability<T>(
        &mut self,
        adapter: Arc<T>,
        claim: Option<OutboundLeafClaimFn<T>>,
    ) where
        T: ProtocolSupportCapability + InboundListenerCapability + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: claim.map(|claim| {
                Arc::new(ProjectedOutboundClaimer {
                    adapter: adapter.clone(),
                    claim,
                }) as Arc<dyn super::OutboundLeafClaimer>
            }),
            #[cfg(feature = "managed-udp-runtime")]
            managed_udp_handlers: None,
        });
    }

    #[cfg(feature = "managed-udp-runtime")]
    pub(crate) fn register_managed_capability<T>(
        &mut self,
        adapter: Arc<T>,
        claim: OutboundLeafClaimFn<T>,
    ) where
        T: ProtocolSupportCapability
            + InboundListenerCapability
            + ManagedUdpHandlerProvider
            + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: Some(Arc::new(ProjectedOutboundClaimer {
                adapter: adapter.clone(),
                claim,
            })),
            managed_udp_handlers: Some(adapter.clone()),
        });
    }

    #[cfg(feature = "managed-udp-runtime")]
    pub(crate) fn managed_udp_handler_providers(
        &self,
    ) -> impl Iterator<Item = &Arc<dyn ManagedUdpHandlerProvider>> {
        self.entries
            .iter()
            .filter_map(|entry| entry.managed_udp_handlers.as_ref())
    }
}
