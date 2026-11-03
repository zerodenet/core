#[cfg(feature = "raw-ip-runtime")]
use std::net::IpAddr;
use std::net::SocketAddr;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_ingress::UdpIngressRuntime;

use super::super::SharedIngressRuntimeServices;

#[derive(Clone)]
pub(crate) struct InboundRouteRuntime {
    pub(in crate::runtime) resources:
        Option<std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>>,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) continuity:
        Option<std::sync::Arc<super::source::DatagramContinuityHost>>,
    accepted_tasks: Option<zero_platform_tokio::AcceptedTasks>,
    pub(super) connection: Option<zero_platform_tokio::contract::context::InboundConnectionFacts>,
    pub(super) tcp_runtime: TcpIngressRuntime,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(super) peer_datagram: Option<super::source::PeerExecution>,
    #[cfg(feature = "managed-stream-runtime")]
    pub(super) mux_udp_continuity: crate::runtime::mux_udp::MuxUdpContinuityRegistry,
    #[cfg(feature = "udp-runtime")]
    pub(super) udp_runtime: UdpIngressRuntime,
}

impl InboundRouteRuntime {
    /// Installed only at the original accepted handler's first poll. This keeps
    /// task registration authority separate from the outbound caller identity.
    pub(in crate::runtime) fn with_accepted_tasks(
        mut self,
        tasks: zero_platform_tokio::AcceptedTasks,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        let caller = tasks.caller()?;
        self.tcp_runtime = self.tcp_runtime.with_accepted_caller(caller);
        self.accepted_tasks = Some(tasks);
        Ok(self)
    }
    pub(in crate::runtime) fn accepted_tasks(&self) -> Option<zero_platform_tokio::AcceptedTasks> {
        self.accepted_tasks.clone()
    }

    #[cfg(test)]
    pub(crate) fn accepted_caller(&self) -> Option<zero_platform_tokio::AcceptedCaller> {
        self.tcp_runtime.accepted_caller()
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn with_caller_native(
        mut self,
        registration: crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration,
    ) -> Self {
        self.tcp_runtime = self.tcp_runtime.with_caller_native(registration);
        self
    }
    pub(crate) fn new(
        shared: SharedIngressRuntimeServices,
        inbound_tag: String,
        source_addr: Option<SocketAddr>,
    ) -> Self {
        let shared = shared.with_current_snapshot();
        let tcp_runtime = shared.tcp_runtime(inbound_tag, source_addr);
        Self {
            resources: None,
            #[cfg(feature = "managed-datagram-runtime")]
            continuity: None,
            accepted_tasks: None,
            connection: None,
            #[cfg(feature = "managed-datagram-runtime")]
            peer_datagram: None,
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
    pub(in crate::runtime) resources:
        Option<std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>>,
    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) continuity:
        Option<std::sync::Arc<super::source::DatagramContinuityHost>>,
    pub(super) shared: SharedIngressRuntimeServices,
    pub(super) inbound_tag: String,
    #[cfg(feature = "raw-ip-runtime")]
    ip_routes: std::sync::Arc<std::sync::Mutex<zero_engine::IpRouteCache>>,
}

impl InboundRouteRuntimeFactory {
    pub(crate) fn with_inbound_instance(
        mut self,
        identity: zero_core::component::ComponentInstanceId,
    ) -> Self {
        self.shared = self.shared.with_inbound_instance(identity);
        self
    }
    pub(crate) fn new(shared: SharedIngressRuntimeServices, inbound_tag: String) -> Self {
        Self {
            resources: None,
            #[cfg(feature = "managed-datagram-runtime")]
            continuity: None,
            shared: shared.for_inbound(inbound_tag.clone()),
            inbound_tag,
            #[cfg(feature = "raw-ip-runtime")]
            ip_routes: Default::default(),
        }
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn packet_route_target(
        &self,
        destination: IpAddr,
        protocol: Option<u8>,
    ) -> crate::inventory::PacketRouteTarget {
        // Packet classification only needs the live engine snapshot. Rebuilding
        // TCP/UDP execution services also prepares component plans, even though
        // none of those execution owners are consumed by this lookup.
        let services = self.shared.tcp_services();
        if services.ensure_configuration_committed().is_err() {
            return crate::inventory::PacketRouteTarget::Block;
        }
        if services.resolver().fake_ip_contains(destination) {
            // A synthetic DNS address is a logical target, never a native
            // packet destination. Restore it in the existing session path
            // before the engine chooses a rule and its outbound route mode.
            // Missing mappings retain that path's explicit failure handling.
            return match protocol {
                Some(zero_stack::packet::IPPROTO_TCP | zero_stack::packet::IPPROTO_UDP) => {
                    crate::inventory::PacketRouteTarget::Flow
                }
                _ => crate::inventory::PacketRouteTarget::Unsupported,
            };
        }
        let engine = services.engine();
        let snapshot = engine.runtime_snapshot();
        let trace = engine
            .evaluate_ip_route_cached_in_snapshot(
                &snapshot,
                destination,
                Some(&self.inbound_tag),
                &mut self
                    .ip_routes
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
            )
            .trace;
        // Group scheduling, endpoint admission and executable capability
        // preparation remain live on every packet; a Flow pin is not a bypass.
        let Ok((resolved, _)) =
            engine.resolve_route_decision_in_snapshot(&snapshot, trace.decision)
        else {
            return crate::inventory::PacketRouteTarget::Unsupported;
        };
        services.protocols().prepare_packet_route_target_admitted(
            snapshot.config(),
            resolved,
            protocol,
            trace.route_mode,
            &zero_engine::EndpointAdmission::from_snapshot(&snapshot),
        )
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn datagram_egress(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.shared.tcp_services().upstream().egress_control()
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
