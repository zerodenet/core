use std::net::SocketAddr;
use std::time::Duration;

use crate::protocol_registry::TcpRuntimeServices;

#[derive(Clone)]
pub(crate) struct TcpIngressRuntime {
    pub(super) services: TcpRuntimeServices,
    pub(super) inbound_tag: String,
    pub(super) source_addr: Option<SocketAddr>,
}

impl TcpIngressRuntime {
    pub(crate) fn new(
        services: TcpRuntimeServices,
        inbound_tag: String,
        source_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            services,
            inbound_tag,
            source_addr,
        }
    }

    pub(crate) fn inbound_tag(&self) -> &str {
        &self.inbound_tag
    }

    pub(crate) fn source_addr(&self) -> Option<SocketAddr> {
        self.source_addr
    }

    pub(crate) fn acquire_principal_device(
        &self,
        auth: Option<&zero_core::SessionAuth>,
    ) -> Result<Option<zero_engine::PrincipalDeviceRegistration>, zero_engine::EngineError> {
        let source_ip = self.source_addr.map(|addr| match addr.ip() {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        });
        self.services
            .engine()
            .acquire_principal_device(auth, source_ip.as_ref())
    }

    pub(crate) fn runtime_services(&self) -> TcpRuntimeServices {
        self.services.clone()
    }

    pub(crate) fn idle_timeout(&self) -> Duration {
        Duration::from_secs(
            self.services
                .config()
                .inbounds
                .iter()
                .find(|i| i.tag == self.inbound_tag)
                .and_then(|i| i.idle_timeout_secs)
                .unwrap_or(300),
        )
    }
}
