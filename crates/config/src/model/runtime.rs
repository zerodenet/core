//! Runtime owner limits and production options.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOptionsConfig {
    /// Per-file bound for component-owned file snapshots. Omission keeps the
    /// legacy exported config shape; production readers use 8 MiB by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_read_max_bytes: Option<usize>,
    /// On-demand physical carriers per native MUX pool. Omission preserves the
    /// exported configuration shape and uses a conservative four-carrier cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_mux_max_carriers: Option<usize>,
    /// Total live or unconfirmed physical carriers in the one Mieru adapter
    /// pool, across every profile and both underlays. Four by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mieru_max_carriers: Option<usize>,
    /// Live HY2 pool entries plus retained original failure groups, across
    /// every profile. On demand, 256 by default; failures are never evicted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_max_entries: Option<usize>,
    /// Actual UDP logical registrations per authenticated HY2 pool entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_max_udp_sessions: Option<usize>,
    /// Queued and fragmented UDP payload bytes shared by all registrations of
    /// one actual authenticated HY2 entry. Four MiB by default, on demand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hysteria2_udp_pending_bytes: Option<usize>,
    /// Original DNS custody shared by Node/bootstrap and Default query owners.
    /// An unconfirmed close still consumes one position; DoQ needs two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_originals: Option<usize>,
    /// Actual registered DNS query tasks shared by both purpose owners.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_queries: Option<usize>,
    /// Actual blocking DNS jobs, retained through their original task join.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_blocking_jobs: Option<usize>,
    /// Actual DNS H2 connection drivers, retained through their original join.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_dns_max_h2_drivers: Option<usize>,
    /// Native Direct egress originals per standard accepted UDP association.
    /// On demand, 64 by default; omission preserves the legacy export shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp_direct_max_sockets: Option<usize>,
    #[serde(default = "default_udp_upstream_idle_timeout_seconds")]
    pub udp_upstream_idle_timeout_seconds: u64,
    /// Maximum number of runtime events retained for replay.
    #[serde(default = "default_event_log_capacity")]
    pub event_log_capacity: usize,
    /// Global URL used by end-to-end outbound latency probes.
    #[serde(default)]
    pub latency_test_url: Option<String>,
    /// Optional crash-recovery snapshot for shared principal quota balances.
    /// Relative paths resolve beside the runtime config.
    #[serde(default)]
    pub principal_quota_state_path: Option<String>,
    #[serde(default)]
    pub network: NetworkOptionsConfig,
    #[serde(default)]
    pub udp: UdpPolicyConfig,
    #[serde(default)]
    pub log: LogConfig,
    /// Optional DNS subsystem configuration. Omit for system resolver.
    #[serde(default)]
    pub dns: Option<DnsConfig>,
    /// Optional declarative TUN inbound managed by the proxy lifecycle.
    #[serde(default)]
    pub tun: Option<TunConfig>,
}

impl Default for RuntimeOptionsConfig {
    fn default() -> Self {
        Self {
            file_read_max_bytes: None,
            native_mux_max_carriers: None,
            mieru_max_carriers: None,
            hysteria2_max_entries: None,
            hysteria2_max_udp_sessions: None,
            hysteria2_udp_pending_bytes: None,
            native_dns_max_originals: None,
            native_dns_max_queries: None,
            native_dns_max_blocking_jobs: None,
            native_dns_max_h2_drivers: None,
            udp_direct_max_sockets: None,
            udp_upstream_idle_timeout_seconds: default_udp_upstream_idle_timeout_seconds(),
            event_log_capacity: default_event_log_capacity(),
            latency_test_url: None,
            principal_quota_state_path: None,
            network: NetworkOptionsConfig::default(),
            udp: UdpPolicyConfig::default(),
            log: LogConfig::default(),
            dns: None,
            tun: None,
        }
    }
}

pub const DEFAULT_LATENCY_TEST_URL: &str = "http://www.gstatic.com/generate_204";
pub const DEFAULT_EVENT_LOG_CAPACITY: usize = 1024;

impl RuntimeOptionsConfig {
    pub fn effective_mieru_max_carriers(&self) -> usize {
        self.mieru_max_carriers.unwrap_or(4)
    }
    pub fn effective_hysteria2_max_entries(&self) -> usize {
        self.hysteria2_max_entries.unwrap_or(256)
    }
    pub fn effective_hysteria2_max_udp_sessions(&self) -> usize {
        self.hysteria2_max_udp_sessions.unwrap_or(4096)
    }
    pub fn effective_hysteria2_udp_pending_bytes(&self) -> usize {
        self.hysteria2_udp_pending_bytes.unwrap_or(4 * 1024 * 1024)
    }
    pub fn effective_native_dns_max_originals(&self) -> usize {
        self.native_dns_max_originals.unwrap_or(16)
    }
    pub fn effective_native_dns_max_queries(&self) -> usize {
        self.native_dns_max_queries.unwrap_or(16)
    }
    pub fn effective_native_dns_max_blocking_jobs(&self) -> usize {
        self.native_dns_max_blocking_jobs.unwrap_or(16)
    }
    pub fn effective_native_dns_max_h2_drivers(&self) -> usize {
        self.native_dns_max_h2_drivers.unwrap_or(16)
    }
    pub fn effective_udp_direct_max_sockets(&self) -> usize {
        self.udp_direct_max_sockets.unwrap_or(64)
    }
    pub fn effective_native_mux_max_carriers(&self) -> usize {
        self.native_mux_max_carriers.unwrap_or(4)
    }

    pub fn effective_file_read_max_bytes(&self) -> usize {
        self.file_read_max_bytes.unwrap_or(8 * 1024 * 1024)
    }

    pub fn effective_latency_test_url(&self) -> &str {
        self.latency_test_url_or(None)
    }

    pub fn latency_test_url_or<'a>(&'a self, fallback: Option<&'a str>) -> &'a str {
        self.latency_test_url
            .as_deref()
            .or(fallback)
            .unwrap_or(DEFAULT_LATENCY_TEST_URL)
    }
}
