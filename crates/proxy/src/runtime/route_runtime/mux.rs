use zero_core::Session;
use zero_engine::EngineError;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
use crate::runtime::udp_ingress::UdpIngressRuntime;

#[derive(Clone)]
pub(crate) struct MuxSubstreamRuntime {
    tcp_runtime: TcpIngressRuntime,
    udp_runtime: UdpIngressRuntime,
    mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry,
}

impl MuxSubstreamRuntime {
    pub(crate) fn new(
        tcp_runtime: TcpIngressRuntime,
        udp_runtime: UdpIngressRuntime,
        mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry,
    ) -> Self {
        Self {
            tcp_runtime,
            udp_runtime,
            mux_udp_continuity,
        }
    }

    pub(crate) fn inbound_tag(&self) -> &str {
        self.tcp_runtime.inbound_tag()
    }

    pub(crate) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.udp_runtime.clone()
    }

    pub(crate) fn mux_udp_continuity(&self) -> crate::runtime::mux_udp::MuxUdpContinuityRegistry {
        self.mux_udp_continuity.clone()
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
    ) -> Result<Option<zero_engine::PrincipalDeviceRegistration>, EngineError> {
        self.tcp_runtime.acquire_principal_device(auth)
    }

    pub(crate) async fn open_tcp_upstream(
        &self,
        session: &mut Session,
    ) -> Result<crate::transport::TcpRouteResult, EngineError> {
        self.tcp_runtime.open_tcp_upstream(session).await
    }
}
