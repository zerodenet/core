use crate::protocol_registry::{TcpRuntimeServices, UdpRuntimeServices};

#[derive(Clone)]
pub(crate) struct UdpIngressRuntime {
    pub(super) tcp_services: TcpRuntimeServices,
    pub(super) services: UdpRuntimeServices,
    pub(super) local_addr: Option<std::net::SocketAddr>,
    pub(super) source_addr: Option<std::net::SocketAddr>,
    listener_stop: Option<crate::runtime::route_runtime::ListenerStop>,
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
        }
    }

    pub(crate) fn with_source_addr(&self, source_addr: Option<std::net::SocketAddr>) -> Self {
        Self {
            tcp_services: self.tcp_services.clone(),
            services: self.services.clone(),
            source_addr,
            local_addr: self.local_addr,
            listener_stop: self.listener_stop.clone(),
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
