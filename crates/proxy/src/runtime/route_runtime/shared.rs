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
    let mut trace = engine.route_trace_in_snapshot_with_inbound_and_resolved_ips(
        snapshot,
        &session.target,
        session.sni.as_deref(),
        session.inbound_tag.as_deref(),
        &[],
    );
    if trace.matched_rule.is_none() && engine.route_requires_resolved_ip_in_snapshot(snapshot) {
        if let Address::Domain(domain) = &session.target {
            if let Ok(resolved) = services.resolver().resolve_real(domain).await {
                let resolved_ips = resolved_route_ips(resolved);
                trace = engine.route_trace_in_snapshot_with_inbound_and_resolved_ips(
                    snapshot,
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

#[cfg(test)]
mod tests {
    use zero_config::RuntimeConfig;
    use zero_core::{Address, Network, ProtocolType, Session};
    use zero_engine::RouteDecision;

    use super::route_trace_for_session;

    #[tokio::test]
    async fn domain_trace_rechecks_resolved_ip_rules() {
        let config = RuntimeConfig::parse(
            r#"{
                "outbounds": [
                    { "tag": "proxy", "protocol": { "type": "direct" } }
                ],
                "route": {
                    "rules": [
                        {
                            "condition": {
                                "type": "ip",
                                "values": ["127.0.0.0/8", "::1/128"]
                            },
                            "action": { "type": "direct" }
                        }
                    ],
                    "final": { "type": "route", "outbound": "proxy" }
                }
            }"#,
        )
        .expect("parse routing config");
        let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
        let session = Session::new(
            1,
            Address::Domain("localhost".to_owned()),
            80,
            Network::Tcp,
            ProtocolType::UNKNOWN,
        );

        let trace = route_trace_for_session(&proxy.tcp_runtime_services(), &session).await;

        assert_eq!(trace.decision, RouteDecision::Direct);
        let matched = trace.matched_rule.expect("resolved IP rule matched");
        assert_eq!(matched.index, 0);
        assert_eq!(matched.condition, "ip: 127.0.0.0/8, ::1/128");
    }
}
