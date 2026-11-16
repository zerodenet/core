use std::sync::Arc;

use super::ProtocolRegistry;
use crate::protocol_registry::{OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability};

type OutboundLeafClaimFn<T> =
    for<'a> fn(&T, OutboundLeafInput<'a>) -> Option<OutboundLeafClaim<'a>>;

#[cfg(any(feature = "socks5", feature = "shadowsocks"))]
type PacketPathProjectionFn<T> =
    fn(
        &T,
        &zero_config::OutboundConfig,
    ) -> Result<Option<super::ColdPacketPathProjection>, zero_engine::EngineError>;

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
            !adapter.has_inbound() || self.inbounds.has_factory(adapter.name()),
            "standard inbound requires its actual component factory"
        );
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            #[cfg(feature = "udp-runtime")]
            packet_projection: None,
            outbound: claim.map(|claim| {
                Arc::new(ProjectedOutboundClaimer {
                    adapter: adapter.clone(),
                    claim,
                }) as Arc<dyn super::OutboundLeafClaimer>
            }),
        });
    }
    /// Existing optional cold projection, separate from executable claims.
    /// Calling it cannot construct TCP/UDP operations or select another pool.
    #[cfg(any(feature = "socks5", feature = "shadowsocks"))]
    pub(crate) fn register_standard_packet_carrier<T>(
        &mut self,
        adapter: Arc<T>,
        claim: Option<OutboundLeafClaimFn<T>>,
        projection: PacketPathProjectionFn<T>,
    ) where
        T: ProtocolSupportCapability + 'static,
    {
        self.register_standard_protocol(adapter.clone(), claim);
        self.entries
            .last_mut()
            .expect("same registered protocol")
            .packet_projection = Some(Arc::new(move |input| projection(adapter.as_ref(), input)));
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
