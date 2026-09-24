use std::sync::Arc;

use super::ProtocolRegistry;
#[cfg(feature = "managed-udp-runtime")]
use crate::protocol_registry::ManagedUdpHandlerProvider;
#[cfg(feature = "upstream-association-runtime")]
use crate::protocol_registry::UpstreamUdpHandlerProvider;
use crate::protocol_registry::{
    InboundListenerCapability, OutboundLeafClaim, OutboundLeafInput, ProtocolSupportCapability,
    TcpOutboundCapability,
};
#[cfg(feature = "udp-runtime")]
use crate::protocol_registry::{UdpFlowCapability, UdpPacketPathCapability};

type OutboundLeafClaimFn<T> =
    for<'a> fn(&T, OutboundLeafInput<'a>) -> Option<OutboundLeafClaim<'a>>;

struct NoOutboundClaimer<T> {
    adapter: Arc<T>,
}

impl<T> super::OutboundLeafClaimer for NoOutboundClaimer<T>
where
    T: TcpOutboundCapability + Send + Sync + 'static,
{
    fn claim_outbound_leaf<'a>(
        &self,
        _input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let _ = &self.adapter;
        None
    }
}

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
        T: ProtocolSupportCapability + InboundListenerCapability + TcpOutboundCapability + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: match claim {
                Some(claim) => Arc::new(ProjectedOutboundClaimer {
                    adapter: adapter.clone(),
                    claim,
                }),
                None => Arc::new(NoOutboundClaimer {
                    adapter: adapter.clone(),
                }),
            },
            #[cfg(test)]
            tcp: adapter,
            #[cfg(all(test, feature = "udp-runtime"))]
            udp: None,
            #[cfg(feature = "managed-udp-runtime")]
            managed_udp_handlers: None,
            #[cfg(feature = "upstream-association-runtime")]
            upstream_udp_handler: None,
            #[cfg(all(test, feature = "udp-runtime"))]
            packet_path: None,
        });
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn register_capability<T>(&mut self, adapter: Arc<T>, claim: OutboundLeafClaimFn<T>)
    where
        T: ProtocolSupportCapability
            + InboundListenerCapability
            + TcpOutboundCapability
            + UdpFlowCapability
            + UdpPacketPathCapability
            + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: Arc::new(ProjectedOutboundClaimer {
                adapter: adapter.clone(),
                claim,
            }),
            #[cfg(test)]
            tcp: adapter.clone(),
            #[cfg(all(test, feature = "udp-runtime"))]
            udp: Some(adapter.clone()),
            #[cfg(feature = "managed-udp-runtime")]
            managed_udp_handlers: None,
            #[cfg(feature = "upstream-association-runtime")]
            upstream_udp_handler: None,
            #[cfg(all(test, feature = "udp-runtime"))]
            packet_path: Some(adapter),
        });
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn register_upstream_capability<T>(
        &mut self,
        adapter: Arc<T>,
        claim: OutboundLeafClaimFn<T>,
    ) where
        T: ProtocolSupportCapability
            + InboundListenerCapability
            + TcpOutboundCapability
            + UdpFlowCapability
            + UdpPacketPathCapability
            + UpstreamUdpHandlerProvider
            + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: Arc::new(ProjectedOutboundClaimer {
                adapter: adapter.clone(),
                claim,
            }),
            #[cfg(test)]
            tcp: adapter.clone(),
            #[cfg(all(test, feature = "udp-runtime"))]
            udp: Some(adapter.clone()),
            #[cfg(feature = "managed-udp-runtime")]
            managed_udp_handlers: None,
            upstream_udp_handler: Some(adapter.clone()),
            #[cfg(all(test, feature = "udp-runtime"))]
            packet_path: Some(adapter),
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
            + TcpOutboundCapability
            + UdpFlowCapability
            + UdpPacketPathCapability
            + ManagedUdpHandlerProvider
            + 'static,
    {
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter.clone(),
            inbound: adapter.clone(),
            outbound: Arc::new(ProjectedOutboundClaimer {
                adapter: adapter.clone(),
                claim,
            }),
            #[cfg(test)]
            tcp: adapter.clone(),
            #[cfg(all(test, feature = "udp-runtime"))]
            udp: Some(adapter.clone()),
            managed_udp_handlers: Some(adapter.clone()),
            #[cfg(feature = "upstream-association-runtime")]
            upstream_udp_handler: None,
            #[cfg(all(test, feature = "udp-runtime"))]
            packet_path: Some(adapter),
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

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_udp_handler_providers(
        &self,
    ) -> impl Iterator<Item = &Arc<dyn UpstreamUdpHandlerProvider>> {
        self.entries
            .iter()
            .filter_map(|entry| entry.upstream_udp_handler.as_ref())
    }
}
