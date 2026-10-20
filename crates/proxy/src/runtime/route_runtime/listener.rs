#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

use super::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

#[derive(Clone)]
pub(crate) struct InboundListenerRuntime {
    ready: Option<crate::runtime::inbound_owners::InboundReady>,
    host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    route_factory: InboundRouteRuntimeFactory,
}

impl InboundListenerRuntime {
    pub(crate) fn with_readiness(
        mut self,
        ready: crate::runtime::inbound_owners::InboundReady,
    ) -> Self {
        self.ready = Some(ready);
        self
    }
    pub(crate) fn readiness(&self) -> Option<crate::runtime::inbound_owners::InboundReady> {
        self.ready.clone()
    }
    #[cfg(any(
        feature = "inbound-listener-group-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) fn split_readiness(
        &self,
        count: usize,
    ) -> Result<Vec<Self>, zero_engine::EngineError> {
        match &self.ready {
            Some(ready) => Ok(ready
                .split(count)?
                .into_iter()
                .map(|ready| self.clone().with_readiness(ready))
                .collect()),
            None => Ok((0..count).map(|_| self.clone()).collect()),
        }
    }

    pub(crate) fn new(
        shared: SharedIngressRuntimeServices,
        inbound_tag: String,
        host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    ) -> Self {
        Self {
            ready: None,
            host,
            route_factory: InboundRouteRuntimeFactory::new(shared, inbound_tag),
        }
    }

    #[cfg(any(
        feature = "inbound-listener-group-runtime",
        feature = "managed-datagram-runtime"
    ))]
    pub(crate) fn host_control(
        &self,
    ) -> Option<&crate::runtime::inbound_owners::InboundTaskAccess> {
        self.host.as_ref()
    }

    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn inbound_tag(&self) -> &str {
        self.route_factory.inbound_tag()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn datagram_egress(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.route_factory.datagram_egress()
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
    owners: std::sync::Arc<crate::runtime::inbound_owners::InboundOwnerRegistry>,
    shared: SharedIngressRuntimeServices,
}

impl InboundListenerRuntimeFactory {
    pub(crate) fn owners(
        &self,
    ) -> &std::sync::Arc<crate::runtime::inbound_owners::InboundOwnerRegistry> {
        &self.owners
    }
    pub(crate) fn endpoint_admission(&self) -> zero_engine::EndpointAdmission<'_> {
        self.shared.endpoint_admission()
    }
    pub(crate) fn new(
        shared: SharedIngressRuntimeServices,
        owners: std::sync::Arc<crate::runtime::inbound_owners::InboundOwnerRegistry>,
    ) -> Self {
        Self { shared, owners }
    }

    pub(crate) fn publish_inbound_terminal(
        &self,
        tag: &str,
        terminal: &crate::runtime::inbound_owners::InboundTerminal,
    ) {
        self.shared.publish_inbound_terminal(tag, terminal);
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
        let host = self
            .owners
            .owner(&inbound_tag)
            .map(|owner| owner.task_access());
        InboundListenerRuntime::new(self.shared.clone(), inbound_tag, host)
    }
}
