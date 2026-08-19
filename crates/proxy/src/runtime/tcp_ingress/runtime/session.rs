use zero_core::{Address, Session};
use zero_engine::{EngineError, SessionHandle};

use super::super::lifecycle::apply_kernel_rate_limits_from_config;
use super::model::TcpIngressRuntime;

impl TcpIngressRuntime {
    pub(crate) async fn resolve_fake_ip_target(&self, session: &mut Session) {
        use zero_traits::IpAddress;

        let ip = match &session.target {
            Address::Ipv4(octets) => IpAddress::V4(*octets),
            Address::Ipv6(octets) => IpAddress::V6(*octets),
            _ => return,
        };
        if let Some(domain) = self.services.resolver().lookup_fake_ip(&ip).await {
            session.target = Address::Domain(domain);
        }
    }

    pub(crate) fn apply_url_rewrite(&self, session: &mut Session) {
        let rules = &self.services.config().route.url_rewrite;
        if rules.is_empty() {
            return;
        }
        let Address::Domain(ref domain) = session.target else {
            return;
        };
        for rule in rules {
            if let Some(ref from) = rule.from {
                if from == domain {
                    session.target = Address::Domain(rule.to.clone());
                    return;
                }
            }
            if let Some(ref pattern) = &rule.from_regex {
                if let Ok(re) = regex::Regex::new(pattern) {
                    if re.is_match(domain) {
                        let result = re.replace(domain, &rule.to);
                        session.target = Address::Domain(result.to_string());
                        return;
                    }
                }
            }
        }
    }

    pub(crate) fn apply_kernel_rate_limits(&self, session: &mut Session) {
        apply_kernel_rate_limits_from_config(self.services.config(), session, &self.inbound_tag);
    }

    pub(crate) async fn prepare_session(&self, session: &mut Session) -> Result<(), EngineError> {
        if let Some(addr) = self.source_addr {
            session.source_ip = Some(match addr.ip() {
                std::net::IpAddr::V4(v4) => Address::Ipv4(v4.octets()),
                std::net::IpAddr::V6(v6) => Address::Ipv6(v6.octets()),
            });
            session.source_port = Some(addr.port());
        }
        if let Some(addr) = self.source_addr {
            if let Some(info) = zero_platform_tokio::lookup_local_tcp_process(addr).await {
                session.process_id = Some(info.pid);
                session.process_name = info.name;
                session.process_path = info.path;
            }
        }

        self.services
            .engine()
            .prepare_session(session, &self.inbound_tag)
    }

    pub(crate) fn track_session(&self, session_id: u64) -> SessionHandle {
        self.services.engine().track_session(session_id)
    }

    pub(crate) fn traffic_rate_limiters(
        &self,
        session: &Session,
    ) -> crate::runtime::principal_rate_limit::TrafficRateLimiters {
        self.services.traffic_rate_limiters(session)
    }
}
