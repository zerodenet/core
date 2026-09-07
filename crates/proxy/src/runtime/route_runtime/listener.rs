#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

use super::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

#[derive(Clone)]
pub(crate) struct InboundListenerRuntime {
    route_factory: InboundRouteRuntimeFactory,
}

impl InboundListenerRuntime {
    pub(crate) fn new(shared: SharedIngressRuntimeServices, inbound_tag: String) -> Self {
        Self {
            route_factory: InboundRouteRuntimeFactory::new(shared, inbound_tag),
        }
    }

    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn inbound_tag(&self) -> &str {
        self.route_factory.inbound_tag()
    }

    pub(crate) fn route_factory(&self) -> InboundRouteRuntimeFactory {
        self.route_factory.clone()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.route_factory.udp_runtime()
    }
}

#[derive(Clone)]
pub(crate) struct InboundListenerRuntimeFactory {
    shared: SharedIngressRuntimeServices,
}

impl InboundListenerRuntimeFactory {
    pub(crate) fn new(shared: SharedIngressRuntimeServices) -> Self {
        Self { shared }
    }

    pub(crate) fn listener_config(
        &self,
        inbound: &zero_config::InboundConfig,
    ) -> zero_config::InboundConfig {
        let mut effective = inbound.clone();
        effective.udp.enabled &= self.shared.udp_enabled();
        effective
    }

    pub(crate) fn for_inbound(&self, inbound_tag: String) -> InboundListenerRuntime {
        InboundListenerRuntime::new(self.shared.clone(), inbound_tag)
    }
}
