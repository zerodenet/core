//! Actual incoming Source work bounds; no task/native grant or second book.
use zero_config::RuntimeOptionsConfig;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawIpSourceLimits {
    pub(crate) tcp: usize,
    pub(crate) udp: usize,
    pub(crate) dns: usize,
    pub(crate) icmp: usize,
    pub(crate) tcp_dns: usize,
}
impl Default for RawIpSourceLimits {
    fn default() -> Self {
        Self {
            tcp: 4096,
            udp: 1024,
            dns: 256,
            icmp: 64,
            tcp_dns: 256,
        }
    }
}
impl RawIpSourceLimits {
    pub(crate) fn from_runtime(r: &RuntimeOptionsConfig) -> Result<Self, ChannelError> {
        let limits = Self {
            tcp: r.effective_raw_ip_max_tcp_connections(),
            udp: r.effective_raw_ip_max_udp_associations(),
            dns: r.effective_raw_ip_max_dns_queries(),
            icmp: r.effective_raw_ip_max_icmp_probes(),
            tcp_dns: r.effective_system_tun_max_tcp_dns_connections(),
        };
        if limits
            .values()
            .into_iter()
            .any(|value| !(1..=65536).contains(&value))
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        Ok(limits)
    }
    pub(crate) fn wire_values(self) -> [usize; 4] {
        [self.tcp, self.udp, self.dns, self.icmp]
    }
    pub(crate) fn values(self) -> [usize; 5] {
        [self.tcp, self.udp, self.dns, self.icmp, self.tcp_dns]
    }
    pub(crate) fn task_limit(self, device: bool) -> Result<usize, ChannelError> {
        // Existing metadata lookups are the extra actual worker per TCP/UDP caller.
        // These maxima are not actual global registrations; sameRegistrar reserves
        // every real task and retains it through its original join and Host ACK.
        self.tcp
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.udp.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.dns))
            .and_then(|n| n.checked_add(zero_stack::UserTcpStack::BACKGROUND_TASK_LIMIT))
            .and_then(|n| n.checked_add(self.icmp))
            .and_then(|n| n.checked_add(3))
            .and_then(|n| n.checked_add(if device { self.tcp_dns } else { 0 }))
            .and_then(|n| n.checked_add(usize::from(device && cfg!(windows))))
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component))
    }
}
