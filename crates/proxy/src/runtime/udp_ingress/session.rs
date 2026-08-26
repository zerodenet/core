use zero_core::Session;
use zero_engine::{EngineError, SessionHandle};
use zero_traits::DnsResolver;

use super::model::UdpIngressRuntime;
use crate::runtime::tcp_ingress::apply_kernel_rate_limits_from_config;

impl UdpIngressRuntime {
    pub(crate) async fn resolve_local_dns(&self, domain: &str) {
        let _ = self.tcp_services.resolver().resolve(domain).await;
    }

    pub(crate) async fn prepare_udp_session(
        &self,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<(), EngineError> {
        if session.source_ip.is_none() {
            if let Some(source_addr) = self.source_addr {
                session.source_ip = Some(match source_addr.ip() {
                    std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                });
                session.source_port = Some(source_addr.port());
            }
        }
        if session.process_id.is_none() {
            if let Some(source_addr) = self.source_addr {
                if let Some(info) = zero_platform_tokio::lookup_local_udp_process(source_addr).await
                {
                    session.process_id = Some(info.pid);
                    session.process_name = info.name;
                    session.process_path = info.path;
                }
            }
        }
        apply_kernel_rate_limits_from_config(self.tcp_services.config(), session, inbound_tag);
        self.tcp_services
            .engine()
            .prepare_session(session, inbound_tag)
    }

    pub(crate) fn track_session(&self, session_id: u64) -> SessionHandle {
        self.tcp_services.engine().track_session(session_id)
    }

    pub(crate) fn traffic_rate_limiters(
        &self,
        session: &Session,
    ) -> crate::runtime::principal_rate_limit::TrafficRateLimiters {
        self.tcp_services.traffic_rate_limiters(session)
    }

    pub(crate) async fn resolve_fake_ip_target(&self, session: &mut Session) {
        crate::runtime::target::resolve_dns_target(self.tcp_services.resolver(), session).await;
    }

    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-stream-runtime"
    ))]
    pub(crate) fn record_session_inbound_traffic(
        &self,
        session_id: u64,
        traffic: crate::transport::StreamTraffic,
    ) {
        self.services
            .record_session_inbound_traffic(session_id, traffic);
    }
}
