use std::net::SocketAddr;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

use super::super::SharedIngressRuntimeServices;

#[derive(Clone)]
pub(crate) struct InboundRouteRuntime {
    pub(super) tcp_runtime: TcpIngressRuntime,
    #[cfg(feature = "managed-stream-runtime")]
    pub(super) mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry,
    #[cfg(feature = "udp-runtime")]
    pub(super) udp_runtime: UdpIngressRuntime,
}

impl InboundRouteRuntime {
    pub(crate) fn new(
        shared: SharedIngressRuntimeServices,
        inbound_tag: String,
        source_addr: Option<SocketAddr>,
    ) -> Self {
        let shared = shared.with_current_snapshot();
        let tcp_runtime = shared.tcp_runtime(inbound_tag, source_addr);
        Self {
            #[cfg(feature = "managed-stream-runtime")]
            mux_udp_continuity: shared.mux_udp_continuity(),
            #[cfg(feature = "udp-runtime")]
            udp_runtime: shared.udp_runtime(),
            tcp_runtime,
        }
    }
}

#[derive(Clone)]
pub(crate) struct InboundRouteRuntimeFactory {
    pub(super) shared: SharedIngressRuntimeServices,
    pub(super) inbound_tag: String,
}

impl InboundRouteRuntimeFactory {
    pub(crate) fn new(shared: SharedIngressRuntimeServices, inbound_tag: String) -> Self {
        Self {
            shared,
            inbound_tag,
        }
    }
}
