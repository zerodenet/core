use std::net::SocketAddr;

#[cfg(feature = "managed-stream-runtime")]
use super::super::MuxSubstreamRuntime;
use super::model::{InboundRouteRuntime, InboundRouteRuntimeFactory};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

impl InboundRouteRuntime {
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

    pub(crate) fn select_http_redirect(
        &self,
        session: &zero_core::Session,
    ) -> Option<(u16, String)> {
        self.tcp_runtime.select_http_redirect(session)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.udp_runtime.with_source_addr(self.source_addr())
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn into_mux_substream_runtime(self) -> MuxSubstreamRuntime {
        let source_addr = self.tcp_runtime.source_addr();
        MuxSubstreamRuntime::new(
            self.tcp_runtime,
            self.udp_runtime.with_source_addr(source_addr),
            self.mux_udp_continuity,
        )
    }
}

impl InboundRouteRuntimeFactory {
    pub(crate) fn inbound_tag(&self) -> &str {
        &self.inbound_tag
    }

    pub(crate) fn for_connection(&self, source_addr: Option<SocketAddr>) -> InboundRouteRuntime {
        InboundRouteRuntime::new(self.shared.clone(), self.inbound_tag.clone(), source_addr)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.shared.udp_runtime()
    }
}
