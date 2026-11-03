use crate::protocol_registry::{TcpRuntimeServices, UdpRuntimeServices};

#[derive(Clone)]
pub(crate) struct UdpIngressRuntime {
    pub(super) tcp_services: TcpRuntimeServices,
    accepted_caller: Option<zero_platform_tokio::AcceptedCaller>,
    pub(super) services: UdpRuntimeServices,
    pub(super) local_addr: Option<std::net::SocketAddr>,
    pub(super) source_addr: Option<std::net::SocketAddr>,
    listener_stop: Option<crate::runtime::route_runtime::ListenerStop>,
    #[cfg(feature = "raw-ip-runtime")]
    native:
        Option<crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration>,
    #[cfg(feature = "managed-datagram-runtime")]
    peer_resources: Option<crate::runtime::inbound_owners::PeerResourceContext>,
    #[cfg(feature = "managed-datagram-runtime")]
    bound_direct_budget: Option<crate::runtime::udp_dispatch::BoundDirectBudget>,
    #[cfg(feature = "managed-datagram-runtime")]
    direct_peer_driver: Option<crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot>,
}

impl UdpIngressRuntime {
    pub(crate) fn accepted_native_socket_limit(&self) -> Option<std::num::NonZeroUsize> {
        // Independent kernel-egress budget, not borrowed from accepted TCP
        // slots, listener bind attempts or the global logical-controller cap.
        (self.accepted_caller.is_some()
            || self.tcp_services.execution().has_datagram_return_owner())
        .then(|| {
            std::num::NonZeroUsize::new(
                self.tcp_services
                    .config()
                    .runtime
                    .effective_udp_direct_max_sockets(),
            )
            .expect("validated independent UDP native budget")
        })
    }
    pub(crate) fn with_accepted_caller(
        mut self,
        caller: zero_platform_tokio::AcceptedCaller,
    ) -> Self {
        self.accepted_caller = Some(caller);
        self
    }
    pub(crate) fn with_datagram_returns(
        mut self,
        returns: std::sync::Arc<crate::component::datagram_retirement::DatagramReturns>,
    ) -> Self {
        self.tcp_services = self.tcp_services.with_datagram_returns(returns);
        self.services = UdpRuntimeServices::new(self.tcp_services.clone());
        self
    }
    pub(crate) fn new(tcp_services: TcpRuntimeServices) -> Self {
        let services = UdpRuntimeServices::new(tcp_services.clone());
        Self {
            tcp_services,
            accepted_caller: None,
            services,
            source_addr: None,
            local_addr: None,
            listener_stop: None,
            #[cfg(feature = "raw-ip-runtime")]
            native: None,
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: None,
            #[cfg(feature = "managed-datagram-runtime")]
            bound_direct_budget: None,
            #[cfg(feature = "managed-datagram-runtime")]
            direct_peer_driver: None,
        }
    }

    pub(crate) fn with_admission(
        mut self,
        session: &zero_core::Session,
        demand: crate::component::business_demand::AdmittedBusinessDemand,
    ) -> Result<Self, zero_engine::EngineError> {
        self.tcp_services =
            self.tcp_services
                .for_inbound(session.inbound_tag.clone().ok_or_else(|| {
                    std::io::Error::other("admitted UDP session has no real inbound")
                })?)
                .with_admitted_business_identity(session, demand)?
                .with_accepted_caller(self.accepted_caller.clone())?;
        self.services = UdpRuntimeServices::new(self.tcp_services.clone());
        Ok(self)
    }

    /// A long-lived UDP association starts each new flow from one current
    /// snapshot. Established flows keep their captured policy and mappings.
    pub(crate) fn with_current_snapshot(&self) -> Self {
        let tcp_services = self.tcp_services.with_current_snapshot();
        Self {
            services: UdpRuntimeServices::new(tcp_services.clone()),
            tcp_services,
            accepted_caller: self.accepted_caller.clone(),
            local_addr: self.local_addr,
            listener_stop: self.listener_stop.clone(),
            #[cfg(feature = "raw-ip-runtime")]
            native: self.native.clone(),
            source_addr: self.source_addr,
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: self.peer_resources.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            bound_direct_budget: self.bound_direct_budget,
            #[cfg(feature = "managed-datagram-runtime")]
            direct_peer_driver: self.direct_peer_driver.clone(),
        }
    }

    pub(crate) fn with_source_addr(&self, source_addr: Option<std::net::SocketAddr>) -> Self {
        Self {
            tcp_services: self.tcp_services.clone(),
            accepted_caller: self.accepted_caller.clone(),
            services: self.services.clone(),
            source_addr,
            local_addr: self.local_addr,
            listener_stop: self.listener_stop.clone(),
            #[cfg(feature = "raw-ip-runtime")]
            native: self.native.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            peer_resources: self.peer_resources.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            bound_direct_budget: self.bound_direct_budget,
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
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn with_native(
        mut self,
        native: Option<
            crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration,
        >,
    ) -> Self {
        self.native = native;
        self
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn native(
        &self,
    ) -> Option<&crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration>
    {
        self.native.as_ref()
    }
    /// Explicit actual caller grant. No default budget is inferred from peer count.
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn with_bound_direct_budget(
        mut self,
        budget: crate::runtime::udp_dispatch::BoundDirectBudget,
    ) -> Self {
        self.bound_direct_budget = Some(budget);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn bound_direct_budget(
        &self,
    ) -> Option<crate::runtime::udp_dispatch::BoundDirectBudget> {
        self.bound_direct_budget
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
