#[cfg(feature = "raw-ip-runtime")]
use std::net::IpAddr;
use std::net::SocketAddr;

#[cfg(feature = "raw-ip-runtime")]
use zero_core::Address;

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

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn packet_route_target(
        &self,
        destination: IpAddr,
        protocol: Option<u8>,
    ) -> crate::inventory::PacketRouteTarget {
        let address = match destination {
            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let shared = self.shared.with_current_snapshot();
        let services = shared.tcp_services();
        let engine = services.engine();
        let snapshot = services.snapshot();
        let decision = engine
            .evaluate_route_in_snapshot(snapshot, &address, None, Some(&self.inbound_tag), &[])
            .trace
            .decision;
        let Ok((resolved, _)) = engine.resolve_route_decision_in_snapshot(snapshot, decision)
        else {
            return crate::inventory::PacketRouteTarget::Unsupported;
        };
        services
            .protocols()
            .prepare_packet_route_target(services.config(), resolved, protocol)
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn icmp_egress_generation(&self) -> u64 {
        self.shared.tcp_services().upstream().egress_generation()
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn icmp_egress(
        &self,
        destination: IpAddr,
    ) -> Option<zero_platform_tokio::EgressInterface> {
        self.shared
            .tcp_services()
            .upstream()
            .egress_for_ip(destination)
    }
}
