use crate::protocol_registry::{TcpRuntimeServices, UdpRuntimeServices};
#[cfg(feature = "managed-datagram-runtime")]
mod direct;
mod inspection;

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
    #[cfg(feature = "managed-datagram-runtime")]
    inline_direct: bool,
    #[cfg(feature = "managed-datagram-runtime")]
    inline_direct_cancel: Option<zero_platform_tokio::contract::CancellationToken>,
    #[cfg(feature = "managed-datagram-runtime")]
    direct_preferred_port: Option<Option<u16>>,
}

impl UdpIngressRuntime {
    #[cfg(test)]
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
    pub(crate) async fn lookup_local_process(
        &self,
        source: std::net::SocketAddr,
    ) -> Result<Option<zero_platform_tokio::LocalProcessInfo>, zero_engine::EngineError> {
        let transport = zero_platform_tokio::LocalProcessTransport::Udp;
        let read_limit = std::num::NonZeroUsize::new(
            self.tcp_services
                .config()
                .runtime
                .effective_file_read_max_bytes(),
        )
        .expect("validated metadata file read budget");
        if let Some(caller) = &self.accepted_caller {
            return caller
                .lookup_local_process(source, transport, read_limit)
                .await
                .map_err(|source| std::io::Error::other(source).into());
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(caller) = &self.native {
            return caller
                .lookup_local_process(source, transport, read_limit)
                .await
                .map_err(|source| std::io::Error::other(source).into());
        }
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(peer) = &self.peer_resources {
            return peer
                .binding()
                .lookup_local_process(source, transport, read_limit)
                .await
                .map_err(|source| std::io::Error::other(source).into());
        }
        Err(
            std::io::Error::other("process metadata lookup has no actual ingress caller Scope")
                .into(),
        )
    }

    pub(crate) fn accepted_caller(&self) -> Option<&zero_platform_tokio::AcceptedCaller> {
        self.accepted_caller.as_ref()
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
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct: false,
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct_cancel: None,
            #[cfg(feature = "managed-datagram-runtime")]
            direct_preferred_port: None,
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
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct: self.inline_direct,
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct_cancel: self.inline_direct_cancel.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            direct_preferred_port: self.direct_preferred_port,
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
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct: self.inline_direct,
            #[cfg(feature = "managed-datagram-runtime")]
            inline_direct_cancel: self.inline_direct_cancel.clone(),
            #[cfg(feature = "managed-datagram-runtime")]
            direct_preferred_port: self.direct_preferred_port,
        }
    }

    #[cfg(test)]
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
