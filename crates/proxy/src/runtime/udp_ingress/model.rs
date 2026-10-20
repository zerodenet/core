use crate::protocol_registry::{TcpRuntimeServices, UdpRuntimeServices};

#[derive(Clone)]
pub(crate) struct UdpIngressRuntime {
    pub(super) tcp_services: TcpRuntimeServices,
    pub(super) services: UdpRuntimeServices,
    pub(super) local_addr: Option<std::net::SocketAddr>,
    pub(super) source_addr: Option<std::net::SocketAddr>,
    listener_stop: Option<crate::runtime::route_runtime::ListenerStop>,
    #[cfg(feature = "managed-datagram-runtime")]
    peer_resources: Option<crate::runtime::inbound_owners::PeerResourceContext>,
    #[cfg(feature = "managed-datagram-runtime")]
    direct_peer_driver: Option<crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot>,
}

impl UdpIngressRuntime {
    pub(crate) fn new(tcp_services: TcpRuntimeServices) -> Self {
        let services = UdpRuntimeServices::new(tcp_services.clone());
        Self {
            tcp_services,
            services,
            source_addr: None,
            local_addr: None,
            listener_stop: None,
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: None,
            #[cfg(feature = "managed-datagram-runtime")]
            direct_peer_driver: None,
        }
    }

    /// A long-lived UDP association starts each new flow from one current
    /// snapshot. Established flows keep their captured policy and mappings.
    pub(crate) fn with_current_snapshot(&self) -> Self {
        let tcp_services = self.tcp_services.with_current_snapshot();
        Self {
            services: UdpRuntimeServices::new(tcp_services.clone()),
            tcp_services,
            local_addr: self.local_addr,
            listener_stop: self.listener_stop.clone(),
            source_addr: self.source_addr,
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: self.peer_resources.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            direct_peer_driver: self.direct_peer_driver.clone(),
        }
    }

    pub(crate) fn with_source_addr(&self, source_addr: Option<std::net::SocketAddr>) -> Self {
        Self {
            tcp_services: self.tcp_services.clone(),
            services: self.services.clone(),
            source_addr,
            local_addr: self.local_addr,
            listener_stop: self.listener_stop.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: self.peer_resources.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            direct_peer_driver: self.direct_peer_driver.clone(),
        }
    }

    pub(crate) fn with_listener_stop(
        mut self,
        stop: crate::runtime::route_runtime::ListenerStop,
    ) -> Self {
        self.listener_stop = Some(stop);
        self
    }

    pub(crate) fn claim_listener_stop(
        &self,
    ) -> Result<Option<crate::runtime::route_runtime::UdpStopGuard>, zero_engine::EngineError> {
        self.listener_stop
            .as_ref()
            .map(|stop| stop.claim())
            .transpose()
    }

    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn peer_ingress(
        &self,
    ) -> Result<zero_core::component::RoleInstanceId, zero_engine::EngineError> {
        self.tcp_services
            .execution()
            .ingress
            .map(|ingress| ingress.role)
            .ok_or_else(|| {
                std::io::Error::other("actual peer ingress identity was not installed").into()
            })
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn with_peer_resources(
        mut self,
        context: crate::runtime::inbound_owners::PeerResourceContext,
    ) -> Self {
        self.peer_resources = Some(context);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn peer_resources(
        &self,
    ) -> Option<&crate::runtime::inbound_owners::PeerResourceContext> {
        self.peer_resources.as_ref()
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn with_direct_peer_driver(
        mut self,
        slot: crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot,
    ) -> Self {
        self.direct_peer_driver = Some(slot);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn direct_peer_driver(
        &self,
    ) -> Option<&crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot> {
        self.direct_peer_driver.as_ref()
    }
    pub(crate) fn with_local_addr(mut self, local: Option<std::net::SocketAddr>) -> Self {
        self.local_addr = local;
        self
    }
    pub(crate) fn services(&self) -> &UdpRuntimeServices {
        &self.services
    }
    pub(crate) fn resolver(&self) -> &zero_dns::DnsSystem {
        self.tcp_services.resolver()
    }

    pub(crate) fn runtime_services(&self) -> UdpRuntimeServices {
        self.services.clone()
    }

    pub(crate) fn source_addr(&self) -> Option<std::net::SocketAddr> {
        self.source_addr
    }
}
