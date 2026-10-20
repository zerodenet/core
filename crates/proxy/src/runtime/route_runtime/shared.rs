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
    pub(super) fn config_handle(&self) -> std::sync::Arc<zero_config::RuntimeConfig> {
        self.tcp_services.snapshot().config().clone()
    }

    pub(super) fn publish_inbound_terminal(
        &self,
        tag: &str,
        terminal: &crate::runtime::inbound_owners::InboundTerminal,
    ) {
        let Some(failure) = terminal.failure() else {
            return;
        };
        for source in terminal.native_bound_failures() {
            tracing::error!(inbound_tag = tag, component = terminal.identity().component.0,
                generation = terminal.identity().generation.0, error = ?source,
                reason = "inbound_native_close_fault_physically_closed",
                "supervisor retained original native close failure after confirmed inbound closure");
        }
        for task in terminal.task_failures() {
            tracing::error!(inbound_tag = tag, component = terminal.identity().component.0,
                generation = terminal.identity().generation.0, task_id = task.id.0,
                task_class = ?task.class, error = %task.source, reason = "inbound_fault_physically_closed",
                "supervisor retained original failed inbound task after physical closure");
        }
        for binding in self.tcp_services.config().endpoint_bindings() {
            if binding.inbound_tags.iter().any(|inbound| inbound == tag) {
                self.tcp_services.engine().record_endpoint_runtime_error(
                    &binding.endpoint_id,
                    &failure.to_string(),
                    true,
                );
            }
        }
        tracing::error!(inbound_tag = tag, component = terminal.identity().component.0,
            joined_tasks = terminal.joined_tasks(), closed_io_receipts = terminal.closed_io_receipts(), error = %failure,
            reason = "inbound_fault_physically_closed", "inbound fault handed to supervisor after confirmed shutdown");
    }
    pub(super) fn for_inbound(mut self, tag: String) -> Self {
        self.tcp_services = self.tcp_services.for_inbound(tag);
        #[cfg(feature = "udp-runtime")]
        {
            self.udp_runtime = UdpIngressRuntime::new(self.tcp_services.clone());
        }
        self
    }
    pub(super) fn endpoint_admission(&self) -> zero_engine::EndpointAdmission<'_> {
        zero_engine::EndpointAdmission::from_snapshot(self.tcp_services.snapshot())
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn tcp_services(&self) -> &TcpRuntimeServices {
        &self.tcp_services
    }

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

    pub(super) fn udp_enabled(&self) -> bool {
        self.tcp_services.config().runtime.udp.enabled
    }

    pub(super) fn with_current_snapshot(&self) -> Self {
        let tcp_services = self.tcp_services.with_current_snapshot();
        Self {
            tcp_services: tcp_services.clone(),
            #[cfg(feature = "managed-stream-runtime")]
            mux_udp_continuity: self.mux_udp_continuity.clone(),
            #[cfg(feature = "udp-runtime")]
            udp_runtime: UdpIngressRuntime::new(tcp_services),
        }
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
    let snapshot = services.snapshot();
    let route_target = session.effective_route_target();
    let evaluation = engine.evaluate_route_in_snapshot(
        snapshot,
        route_target,
        session.sni.as_deref(),
        session.inbound_tag.as_deref(),
        &[],
    );
    let mut trace = evaluation.trace;
    if evaluation.needs_resolution {
        if let Address::Domain(domain) = route_target {
            if let Ok(resolved) = services.resolver().resolve_real(domain).await {
                let resolved_ips = resolved_route_ips(resolved);
                trace = engine.route_trace_in_snapshot_with_inbound_and_resolved_ips(
                    snapshot,
                    route_target,
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

#[cfg(test)]
#[path = "shared/tests.rs"]
mod tests;
