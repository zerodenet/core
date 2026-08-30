use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use super::RuleConditionConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    /// DNS backends keyed by stable, client-selected names.
    pub servers: BTreeMap<String, DnsServerConfig>,

    /// Backend used when no dispatch condition matches.
    pub default_server: String,

    /// Ordered DNS dispatch rules. First match wins.
    #[serde(default)]
    pub dispatch: Vec<DnsDispatchRuleConfig>,

    /// Optional TTL-based DNS cache.
    #[serde(default)]
    pub cache: Option<DnsCacheConfig>,

    /// Optional bounded real-IP to domain index for transparent traffic.
    #[serde(default)]
    pub reverse_mapping: Option<DnsReverseMappingConfig>,

    /// Address-answer behavior for intercepted DNS requests.
    #[serde(default)]
    pub answer: DnsAnswerConfig,

    /// Runtime policy shared by every configured DNS transport.
    #[serde(default)]
    pub policy: DnsPolicyConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsPolicyConfig {
    /// Per-backend query deadline in milliseconds.
    #[serde(default = "default_dns_timeout_ms")]
    pub timeout_ms: u64,
    /// Optional per-backend query deadline overrides, keyed by server tag.
    #[serde(default)]
    pub server_timeout_ms: BTreeMap<String, u64>,
    /// Ordered backend tags tried after the dispatch-selected backend fails.
    #[serde(default)]
    pub fallback_servers: Vec<String>,
    /// Backend used to resolve proxy-node and carrier endpoint domains.
    /// Omit to retain the dispatch/default backend behavior.
    #[serde(default)]
    pub node_server: Option<String>,
    /// Ordered fallback chain isolated to proxy-node resolution.
    #[serde(default)]
    pub node_fallback_servers: Vec<String>,
    /// Backend used for targets routed through the direct outbound.
    /// Omit to retain the dispatch/default backend behavior.
    #[serde(default)]
    pub direct_server: Option<String>,
    /// Ordered fallback chain isolated to direct-target resolution.
    #[serde(default)]
    pub direct_fallback_servers: Vec<String>,
    /// Address ranges that make a backend response unusable. A rejected
    /// response advances to the next configured fallback instead of entering
    /// the cache or reverse index.
    #[serde(default)]
    pub reject_address_cidrs: Vec<IpNet>,
    /// Which address families are queried and their result preference.
    #[serde(default)]
    pub address_family: DnsAddressFamilyPolicy,
}

impl Default for DnsPolicyConfig {
    fn default() -> Self {
        Self {
            timeout_ms: default_dns_timeout_ms(),
            server_timeout_ms: BTreeMap::new(),
            fallback_servers: Vec::new(),
            node_server: None,
            node_fallback_servers: Vec::new(),
            direct_server: None,
            direct_fallback_servers: Vec::new(),
            reject_address_cidrs: Vec::new(),
            address_family: DnsAddressFamilyPolicy::default(),
        }
    }
}

impl DnsPolicyConfig {
    pub fn timeout_ms_for(&self, server: &str) -> u64 {
        self.server_timeout_ms
            .get(server)
            .copied()
            .unwrap_or(self.timeout_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DnsAddressFamilyPolicy {
    Ipv4Only,
    Ipv6Only,
    #[default]
    PreferIpv4,
    PreferIpv6,
}

impl DnsAddressFamilyPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ipv4Only => "ipv4_only",
            Self::Ipv6Only => "ipv6_only",
            Self::PreferIpv4 => "prefer_ipv4",
            Self::PreferIpv6 => "prefer_ipv6",
        }
    }
}

const fn default_dns_timeout_ms() -> u64 {
    5_000
}

impl DnsConfig {
    /// Return statically configured DNS endpoint addresses that can be safely
    /// excluded from a TUN default route without recursive resolution.
    ///
    /// System resolver endpoints are discovered by the platform runtime and
    /// merged into this list before TUN capture routes are installed.
    pub fn tun_route_exclusion_addresses(&self) -> Result<Vec<IpAddr>, String> {
        let mut addresses = BTreeSet::new();
        for (tag, server) in &self.servers {
            if server.detour().is_some() {
                continue;
            }
            if matches!(server, DnsServerConfig::System) {
                continue;
            }

            let endpoint_addresses = server.endpoint_addresses().map_err(|error| {
                format!("TUN DNS backend `{tag}` cannot be excluded safely: {error}")
            })?;
            addresses.extend(endpoint_addresses);
        }
        Ok(addresses.into_iter().collect())
    }

    /// Whether TUN route preparation must discover host resolver endpoints.
    pub fn uses_system_dns(&self) -> bool {
        self.servers
            .values()
            .any(|server| matches!(server, DnsServerConfig::System))
    }

    pub fn fake_ip(&self) -> Option<FakeIpConfigRef<'_>> {
        match &self.answer {
            DnsAnswerConfig::Real => None,
            DnsAnswerConfig::FakeIp {
                cidr,
                ipv6_cidr,
                ttl_seconds,
                max_entries,
                exclude_domains,
            } => Some(FakeIpConfigRef {
                cidr,
                ipv6_cidr: ipv6_cidr.as_deref(),
                ttl_seconds: *ttl_seconds,
                max_entries: *max_entries,
                exclude_domains,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FakeIpConfigRef<'a> {
    pub cidr: &'a str,
    pub ipv6_cidr: Option<&'a str>,
    pub ttl_seconds: u64,
    pub max_entries: Option<usize>,
    pub exclude_domains: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum DnsAnswerConfig {
    #[default]
    #[serde(rename = "real")]
    Real,
    #[serde(rename = "fake_ip")]
    FakeIp {
        /// CIDR block for the fake-IP pool, for example `198.18.0.0/15`.
        #[serde(default = "default_fake_ip_cidr")]
        cidr: String,
        /// Optional IPv6 CIDR block for synthetic AAAA answers.
        #[serde(default)]
        ipv6_cidr: Option<String>,
        /// Mapping lifetime in seconds.
        #[serde(default = "default_fake_ip_ttl")]
        ttl_seconds: u64,
        /// Maximum live mappings. Omit to use the smaller of the pool size
        /// and the kernel's bounded default.
        #[serde(default)]
        max_entries: Option<usize>,
        /// Domains that always receive real DNS answers.
        #[serde(default)]
        exclude_domains: Vec<String>,
    },
}

const fn default_fake_ip_ttl() -> u64 {
    86400
}

fn default_fake_ip_cidr() -> String {
    "198.18.0.0/15".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum DnsServerConfig {
    /// System resolver (getaddrinfo).
    #[serde(rename = "system")]
    System,
    /// Plain UDP DNS.
    #[serde(rename = "udp")]
    Udp {
        host: String,
        #[serde(default = "default_dns_port")]
        port: u16,
        #[serde(default)]
        bootstrap: Vec<IpAddr>,
        /// Optional route target used to carry DNS-over-TCP to this server.
        #[serde(default)]
        detour: Option<String>,
    },
    /// DNS-over-HTTPS.
    #[serde(rename = "doh")]
    Doh {
        host: String,
        #[serde(default = "default_dns_https_port")]
        port: u16,
        #[serde(default = "default_doh_path")]
        path: String,
        #[serde(default)]
        bootstrap: Vec<IpAddr>,
        #[serde(default)]
        server_name: Option<String>,
        /// Optional route target used to carry the DoH TCP connection.
        #[serde(default)]
        detour: Option<String>,
    },
    /// DNS-over-TLS.
    #[serde(rename = "dot")]
    Dot {
        host: String,
        #[serde(default = "default_dns_dot_port")]
        port: u16,
        #[serde(default)]
        bootstrap: Vec<IpAddr>,
        #[serde(default)]
        server_name: Option<String>,
        /// Optional route target used to carry the DoT TCP connection.
        #[serde(default)]
        detour: Option<String>,
    },
    /// DNS-over-QUIC (RFC 9250).
    #[serde(rename = "doq")]
    Doq {
        host: String,
        #[serde(default = "default_dns_doq_port")]
        port: u16,
        #[serde(default)]
        bootstrap: Vec<IpAddr>,
        #[serde(default)]
        server_name: Option<String>,
        /// Reserved for a future proxy-aware UDP/QUIC carrier. Validation
        /// currently rejects this field instead of leaking DoQ directly.
        #[serde(default)]
        detour: Option<String>,
    },
}

impl DnsServerConfig {
    pub fn host(&self) -> Option<&str> {
        match self {
            Self::System => None,
            Self::Udp { host, .. }
            | Self::Doh { host, .. }
            | Self::Dot { host, .. }
            | Self::Doq { host, .. } => Some(host),
        }
    }

    pub fn port(&self) -> Option<u16> {
        match self {
            Self::System => None,
            Self::Udp { port, .. }
            | Self::Doh { port, .. }
            | Self::Dot { port, .. }
            | Self::Doq { port, .. } => Some(*port),
        }
    }

    pub fn bootstrap(&self) -> &[IpAddr] {
        match self {
            Self::System => &[],
            Self::Udp { bootstrap, .. }
            | Self::Doh { bootstrap, .. }
            | Self::Dot { bootstrap, .. }
            | Self::Doq { bootstrap, .. } => bootstrap,
        }
    }

    pub fn detour(&self) -> Option<&str> {
        match self {
            Self::System => None,
            Self::Udp { detour, .. }
            | Self::Doh { detour, .. }
            | Self::Dot { detour, .. }
            | Self::Doq { detour, .. } => detour.as_deref(),
        }
    }

    pub fn endpoint_addresses(&self) -> Result<Vec<IpAddr>, String> {
        if matches!(self, Self::System) {
            return Err("system resolver has no deterministic endpoint".to_owned());
        }
        if !self.bootstrap().is_empty() {
            return Ok(self.bootstrap().to_vec());
        }
        let host = self.host().expect("network DNS server has a host");
        host.parse::<IpAddr>()
            .map(|address| vec![address])
            .map_err(|_| format!("domain host `{host}` requires at least one bootstrap address"))
    }
}

const fn default_dns_port() -> u16 {
    53
}

const fn default_dns_https_port() -> u16 {
    443
}

const fn default_dns_dot_port() -> u16 {
    853
}

const fn default_dns_doq_port() -> u16 {
    853
}

fn default_doh_path() -> String {
    "/dns-query".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsCacheConfig {
    /// Max cached domain entries. Default 256.
    #[serde(default = "default_dns_cache_max_entries")]
    pub max_entries: usize,
    /// Cap TTL at this value (seconds). Omit to use DNS record TTL.
    #[serde(default)]
    pub max_ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsReverseMappingConfig {
    /// Maximum distinct real IP addresses retained by the reverse index.
    #[serde(default = "default_dns_reverse_max_entries")]
    pub max_entries: usize,
    /// Maximum candidate domains retained for one shared real IP address.
    /// Lookups with more than one live candidate are intentionally ambiguous.
    #[serde(default = "default_dns_reverse_max_domains_per_address")]
    pub max_domains_per_address: usize,
    /// Upper bound for retained DNS record TTLs.
    #[serde(default = "default_dns_reverse_max_ttl_seconds")]
    pub max_ttl_seconds: u64,
}

const fn default_dns_cache_max_entries() -> usize {
    256
}

const fn default_dns_reverse_max_entries() -> usize {
    1024
}

const fn default_dns_reverse_max_domains_per_address() -> usize {
    8
}

const fn default_dns_reverse_max_ttl_seconds() -> u64 {
    300
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsDispatchRuleConfig {
    pub condition: RuleConditionConfig,
    pub server: String,
}
