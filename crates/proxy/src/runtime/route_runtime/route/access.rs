use std::net::SocketAddr;

#[cfg(feature = "managed-stream-runtime")]
use super::super::MuxSubstreamRuntime;
use super::model::{InboundRouteRuntime, InboundRouteRuntimeFactory};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

impl InboundRouteRuntime {
    #[cfg(feature = "udp-runtime")]
    #[cfg(test)]
    pub(crate) fn with_listener_stop(mut self, stop: super::super::ListenerStop) -> Self {
        self.udp_runtime = self.udp_runtime.with_listener_stop(stop);
        self
    }

    pub(crate) fn upstream_services(&self) -> crate::protocol_registry::UpstreamConnectServices {
        self.tcp_runtime.runtime_services().upstream()
    }
    pub(crate) fn with_local_addr(mut self, local: Option<SocketAddr>) -> Self {
        self.tcp_runtime = self.tcp_runtime.with_local_addr(local);
        self
    }

    pub(crate) fn with_source_addr(mut self, source: Option<SocketAddr>) -> Self {
        self.tcp_runtime = self.tcp_runtime.with_source_addr(source);
        self
    }
    pub(crate) fn inbound_tag(&self) -> &str {
        self.tcp_runtime.inbound_tag()
    }

    pub(crate) fn source_addr(&self) -> Option<SocketAddr> {
        self.tcp_runtime.source_addr()
    }

    pub(crate) fn register_principal_cancellation<F>(
        &self,
        principal_key: &str,
        callback: F,
    ) -> zero_engine::PrincipalCancellationRegistration
    where
        F: FnOnce(String) + Send + 'static,
    {
        self.tcp_runtime
            .runtime_services()
            .engine()
            .register_principal_cancellation(principal_key, callback)
    }

    pub(crate) fn acquire_principal_device(
        &self,
        auth: Option<&zero_core::SessionAuth>,
    ) -> Result<Option<zero_engine::PrincipalDeviceRegistration>, zero_engine::EngineError> {
        self.tcp_runtime.acquire_principal_device(auth)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.udp_runtime
            .with_source_addr(self.source_addr())
            .with_local_addr(self.tcp_runtime.local_addr())
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn into_mux_substream_runtime(self) -> MuxSubstreamRuntime {
        let source_addr = self.tcp_runtime.source_addr();
        let local_addr = self.tcp_runtime.local_addr();
        MuxSubstreamRuntime::new(
            self.tcp_runtime,
            self.udp_runtime
                .with_source_addr(source_addr)
                .with_local_addr(local_addr),
            self.mux_udp_continuity,
        )
    }
}

impl InboundRouteRuntimeFactory {
    #[cfg(feature="raw-ip-runtime")]
    pub(in crate::runtime) fn tun_dns_resolver(&self)->std::sync::Arc<zero_dns::DnsSystem> {
        self.shared.tcp_services().resolver_handle()
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime) fn bind_actual_packet_source(
        &self,
        original: zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
    ) -> Result<(), zero_engine::EngineError> {
        let services = self.shared.tcp_services();
        let execution = services.execution();
        let plan = execution.components.as_deref().map_err(|error| {
            std::io::Error::other(crate::component::instance::retained(error.clone()))
        })?;
        services.protocols().bind_actual_packet_source(
            services.config(),
            plan,
            &self.inbound_tag,
            original,
        )
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn outbound_inner_io(
        &self,
        tag: &str,
    ) -> Option<std::sync::Arc<dyn zero_traits::IoObserver>> {
        crate::runtime::traffic_io::outbound(
            self.shared.tcp_services().engine(),
            tag,
            zero_api::TrafficPlane::Inner,
        )
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn global_packet_traffic(&self) -> Option<zero_engine::TrafficMeter> {
        self.shared
            .tcp_services()
            .engine()
            .traffic_meter(&zero_api::TrafficScope::Global)
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn inbound_packet_traffic(&self) -> Option<zero_engine::TrafficMeter> {
        self.shared
            .tcp_services()
            .engine()
            .traffic_meter(&zero_api::TrafficScope::Inbound {
                tag: self.inbound_tag.clone(),
            })
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn packet_statistics_pins(&self) -> crate::runtime::packet_route::PacketSessionPins {
        let engine = self.shared.tcp_services().engine().clone();
        let inbound = self.inbound_tag.clone();
        let owner = engine.clone();
        let owner_inbound = inbound.clone();
        crate::runtime::packet_route::PacketSessionPins::with_meters(std::sync::Arc::new(
            move |tag, inbound_peer, outbound_peer| {
                engine.packet_route_traffic_meters(&inbound, tag, inbound_peer, outbound_peer)
            },
        ))
        .managed(owner, owner_inbound)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn retain_admitted_packet_pins(
        &self,
        pins: &mut crate::runtime::packet_route::PacketSessionPins,
    ) {
        use crate::runtime::packet_route::PacketPlane;
        let (inbound, outbound) = self
            .shared
            .tcp_services()
            .engine()
            .confirmed_endpoint_packet_revocations();
        pins.retain_admitted(|plane| {
            !inbound.contains(&self.inbound_tag)
                && match plane {
                    PacketPlane::Packet(tag) | PacketPlane::TranslatedPacket(tag) => {
                        !outbound.contains(tag)
                    }
                    _ => true,
                }
        });
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn endpoint_traffic(
        &self,
        peers: &[String],
    ) -> Option<zero_engine::PreparedEndpointTraffic> {
        let services = self.shared.tcp_services();
        let engine = services.engine();
        let binding = engine
            .config()
            .endpoint_bindings()
            .into_iter()
            .find(|b| b.canonical && b.inbound_tags.contains(&self.inbound_tag))?;
        engine.prepare_endpoint_traffic(&binding, peers)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn ensure_configuration_committed(&self) -> Result<(), zero_engine::EngineError> {
        self.shared.tcp_services().ensure_configuration_committed()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn endpoint_inbound_allowed(&self) -> bool {
        if self.ensure_configuration_committed().is_err() {
            return false;
        }
        self.shared
            .tcp_services()
            .engine()
            .endpoint_inbound_allowed(&self.inbound_tag)
    }

    pub(crate) fn inbound_tag(&self) -> &str {
        &self.inbound_tag
    }

    pub(crate) fn for_connection(&self, source_addr: Option<SocketAddr>) -> InboundRouteRuntime {
        let mut route =
            InboundRouteRuntime::new(self.shared.clone(), self.inbound_tag.clone(), source_addr);
        route.resources = self.resources.clone();
        #[cfg(feature = "managed-datagram-runtime")]
        let route = {
            let mut route = route;
            route.continuity = self.continuity.clone();
            route
        };
        route
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.shared.with_current_snapshot().udp_runtime()
    }
}
