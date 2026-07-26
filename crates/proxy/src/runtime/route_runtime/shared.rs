use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::tcp_ingress::TcpIngressRuntime;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;
use zero_core::{Address, Session};
use zero_engine::RouteTrace;
use zero_traits::IpAddress;

#[derive(Clone)]
pub(crate) struct SharedIngressRuntimeServices {
    tcp_services: TcpRuntimeServices,
    #[cfg(feature = "managed-stream-runtime")]
    mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry,
    #[cfg(feature = "udp-runtime")]
    udp_runtime: UdpIngressRuntime,
}

impl SharedIngressRuntimeServices {
    pub(crate) fn new(tcp_services: TcpRuntimeServices) -> Self {
        Self {
            tcp_services: tcp_services.clone(),
            #[cfg(feature = "managed-stream-runtime")]
            mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry::default(),
            #[cfg(feature = "udp-runtime")]
            udp_runtime: UdpIngressRuntime::new(tcp_services),
        }
    }

    pub(super) fn tcp_runtime(
        &self,
        inbound_tag: String,
        source_addr: Option<SocketAddr>,
    ) -> TcpIngressRuntime {
        TcpIngressRuntime::new(self.tcp_services.clone(), inbound_tag, source_addr)
    }

    #[cfg(feature = "udp-runtime")]
    pub(super) fn udp_runtime(&self) -> UdpIngressRuntime {
        self.udp_runtime.clone()
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(super) fn mux_udp_continuity(&self) -> crate::runtime::mux_udp::MuxUdpContinuityRegistry {
        self.mux_udp_continuity.clone()
    }
}

pub(crate) async fn route_trace_for_session(
    services: &TcpRuntimeServices,
    session: &Session,
) -> RouteTrace {
    let engine = services.engine();
    let mut trace = engine.route_trace_with_inbound(
        &session.target,
        session.sni.as_deref(),
        session.inbound_tag.as_deref(),
    );
    if trace.matched_rule.is_none() && engine.route_requires_resolved_ip() {
        if let Address::Domain(domain) = &session.target {
            if let Ok(resolved) = services.resolver().resolve_real(domain).await {
                let resolved_ips = resolved_route_ips(resolved);
                trace = engine.route_trace_with_inbound_and_resolved_ips(
                    &session.target,
                    session.sni.as_deref(),
                    session.inbound_tag.as_deref(),
                    &resolved_ips,
                );
            }
        }
    }
    trace
}

fn resolved_route_ips(addresses: impl IntoIterator<Item = IpAddress>) -> Vec<IpAddr> {
    addresses
        .into_iter()
        .map(|address| match address {
            IpAddress::V4(bytes) => IpAddr::V4(Ipv4Addr::from(bytes)),
            IpAddress::V6(bytes) => IpAddr::V6(Ipv6Addr::from(bytes)),
        })
        .collect()
}
