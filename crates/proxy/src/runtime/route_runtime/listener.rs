#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

use super::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

#[derive(Clone)]
pub(crate) struct InboundListenerRuntime {
    ready: Option<crate::runtime::inbound_owners::InboundReady>,
    peer: Option<crate::runtime::inbound_owners::component::PeerSourceContext>,
    host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    route_factory: InboundRouteRuntimeFactory,
}

impl InboundListenerRuntime {
    pub(in crate::runtime) fn with_datagram_source(
        mut self,
        peer: Option<crate::runtime::inbound_owners::component::PeerSourceContext>,
    ) -> Self {
        self.peer = peer;
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) fn datagram_source(
        &self,
    ) -> Option<crate::runtime::inbound_owners::component::PeerSourceContext> {
        self.peer.clone()
    }

    pub(in crate::runtime) fn has_packet_source(&self) -> bool {
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.host.as_ref().is_some_and(|h| h.has_packet_source())
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            false
        }
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn take_packet_source(
        &self,
    ) -> Option<crate::runtime::inbound_operation::raw_ip::PacketSourceContext> {
        self.host.as_ref()?.packet_source()
    }
    /// Bind the same typed source owner while preserving its real routing tag.
    /// System TUN's registry reservation is not a configured inbound tag.
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn with_source_owner(
        mut self,
        owner: &crate::runtime::inbound_owners::InboundOwner,
    ) -> Self {
        self.host = Some(owner.task_access());
        self.route_factory.resources = owner.inbound_resources();
        self.route_factory = self.route_factory.with_inbound_instance(owner.identity());
        self.ready = Some(owner.readiness());
        self
    }
    pub(crate) fn with_inbound_instance(
        mut self,
        identity: zero_core::component::ComponentInstanceId,
    ) -> Self {
        self.route_factory = self.route_factory.with_inbound_instance(identity);
        self
    }
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

    pub(crate) fn new(
        shared: SharedIngressRuntimeServices,
        inbound_tag: String,
        host: Option<crate::runtime::inbound_owners::InboundTaskAccess>,
    ) -> Self {
        Self {
            ready: None,
            peer: None,
            host,
            route_factory: InboundRouteRuntimeFactory::new(shared, inbound_tag),
        }
    }

    pub(crate) fn host_control(
        &self,
    ) -> Option<&crate::runtime::inbound_owners::InboundTaskAccess> {
        self.host.as_ref()
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
    restoring: bool,
    restart_sources: bool,
    preparing: Option<std::sync::Arc<crate::runtime::inbound_owners::PreparingInbounds>>,
    owners: std::sync::Arc<crate::runtime::inbound_owners::InboundOwnerRegistry>,
    shared: SharedIngressRuntimeServices,
}

impl InboundListenerRuntimeFactory {
    pub(crate) fn for_source_network_restart(mut self) -> Self {
        self.restart_sources = true;
        self
    }
    pub(crate) fn source_network_restart(&self) -> bool {
        self.restart_sources
    }
    pub(crate) fn with_prepared_components(&self) -> Result<Self, zero_engine::EngineError> {
        let mut factory = self.clone();
        factory.shared = self.shared.with_prepared_components()?;
        Ok(factory)
    }
    /// Only last-known-good compensation selects restoration; equal inbound
    /// fields in an ordinary candidate may still change its dependencies.
    pub(crate) fn for_recovery(mut self) -> Self {
        self.preparing = None;
        self.restoring = true;
        self
    }
    pub(crate) fn is_restoring(&self) -> bool {
        self.restoring
    }

    pub(in crate::runtime) fn source_dependencies_changed(
        &self,
        input: &zero_config::InboundConfig,
    ) -> Result<bool, zero_engine::EngineError> {
        if self.restart_sources {
            return Ok(true);
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.shared.source_dependencies_changed(&self.owners, input)
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            let _ = input;
            Ok(false)
        }
    }

    pub(crate) fn config_handle(&self) -> std::sync::Arc<zero_config::RuntimeConfig> {
        self.shared.config_handle()
    }

    pub(crate) fn with_preparing(
        mut self,
        preparing: std::sync::Arc<crate::runtime::inbound_owners::PreparingInbounds>,
    ) -> Self {
        self.preparing = Some(preparing);
        self
    }
    pub(crate) fn without_preparing(mut self) -> Self {
        self.preparing = None;
        self.restart_sources = false;
        self
    }
    pub(crate) fn register_preparing(
        &self,
        owner: &std::sync::Arc<crate::runtime::inbound_owners::InboundOwner>,
    ) -> Result<(), zero_engine::EngineError> {
        owner.bind_configuration_publication(
            &self.config_handle(),
            self.shared.configuration_publication(),
        )?;
        self.preparing
            .as_ref()
            .map_or(Ok(()), |preparing| preparing.register(owner))
    }

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
        Self {
            shared,
            owners,
            restoring: false,
            restart_sources: false,
            preparing: None,
        }
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
        let mut runtime =
            InboundListenerRuntime::new(self.shared.clone(), inbound_tag.clone(), host);
        if let Some(owner) = self.owners.owner(&inbound_tag) {
            runtime.route_factory.resources = owner.inbound_resources();
        }
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(owner) = self.owners.owner(&inbound_tag) {
            runtime.route_factory.continuity = owner.datagram_continuity();
        }
        runtime
    }
}
