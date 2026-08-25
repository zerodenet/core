use alloc::string::String;

use crate::address::Address;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetHostSource {
    FakeIp,
    DnsReverse,
    HttpHost,
    QuicSni,
    TlsSni,
}

impl TargetHostSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FakeIp => "fake_ip",
            Self::DnsReverse => "dns_reverse",
            Self::HttpHost => "http_host",
            Self::QuicSni => "quic_sni",
            Self::TlsSni => "tls_sni",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeIpReverseStatus {
    Resolved,
    Missing,
}

impl FakeIpReverseStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::Missing => "missing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolType(&'static str);

impl ProtocolType {
    pub const UNKNOWN: Self = Self("unknown");

    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAuth {
    pub scheme: String,
    pub principal_key: Option<String>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    /// Maximum number of distinct concurrently active source IP addresses.
    /// `None` means unlimited.
    pub device_limit: Option<u32>,
    /// Shared remaining traffic budget for this policy revision.
    pub quota_remaining_bytes: Option<u64>,
    /// Monotonic principal policy revision that owns the quota snapshot.
    pub policy_revision: Option<u64>,
}

impl SessionAuth {
    pub fn new(scheme: impl Into<String>) -> Self {
        Self {
            scheme: scheme.into(),
            principal_key: None,
            up_bps: None,
            down_bps: None,
            device_limit: None,
            quota_remaining_bytes: None,
            policy_revision: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub id: u64,
    pub inbound_tag: Option<String>,
    pub outbound_tag: Option<String>,
    pub target: Address,
    /// Destination used only when the selected outbound is direct.
    ///
    /// Transparent inbounds may recover a logical hostname for routing while
    /// retaining the client-selected IP for the actual direct socket. Proxy
    /// outbounds intentionally continue to receive [`Self::target`].
    pub direct_target: Option<Address>,
    /// Original IP target before Fake-IP restoration or content sniffing.
    pub original_target: Option<Address>,
    /// Source used to recover the current domain target.
    pub target_host_source: Option<TargetHostSource>,
    /// Fake-IP reverse lookup result when the original target was in the
    /// configured synthetic pool.
    pub fake_ip_reverse_status: Option<FakeIpReverseStatus>,
    /// Whether an IP target came from transparent interception and may be
    /// safely recovered through the DNS real-IP reverse index.
    pub transparent_target: bool,
    pub port: u16,
    pub network: Network,
    pub protocol: ProtocolType,
    pub auth: Option<SessionAuth>,
    /// Upload rate limit in bytes/s. Authenticated sessions with the same Zero
    /// principal policy share one aggregate timeline; anonymous sessions use
    /// an independent timeline. `None` = unlimited.
    pub up_bps: Option<u64>,
    /// Download counterpart of [`Self::up_bps`]. `None` = unlimited.
    pub down_bps: Option<u64>,
    /// TLS Server Name Indication from ClientHello, if peeked.
    pub sni: Option<String>,
    /// Client's source IP, if available from the inbound listener.
    pub source_ip: Option<Address>,
    /// Client's source port, if available.
    pub source_port: Option<u16>,
    /// Local process ID that initiated this connection (Linux only).
    pub process_id: Option<u32>,
    /// Local process name (Linux only).
    pub process_name: Option<String>,
    /// Local process executable path (Linux only).
    pub process_path: Option<String>,
}

impl Session {
    pub fn new(
        id: u64,
        target: Address,
        port: u16,
        network: Network,
        protocol: ProtocolType,
    ) -> Self {
        Self {
            id,
            inbound_tag: None,
            outbound_tag: None,
            target,
            direct_target: None,
            original_target: None,
            target_host_source: None,
            fake_ip_reverse_status: None,
            transparent_target: false,
            port,
            network,
            protocol,
            auth: None,
            up_bps: None,
            down_bps: None,
            sni: None,
            source_ip: None,
            source_port: None,
            process_id: None,
            process_name: None,
            process_path: None,
        }
    }

    pub fn effective_direct_target(&self) -> &Address {
        self.direct_target.as_ref().unwrap_or(&self.target)
    }

    /// Apply authenticated user identity and rate limits to this session.
    ///
    /// Every protocol handler should call this once after authentication,
    /// before `prepare_session`.  All the common wiring (principal_key,
    /// up_bps, down_bps) happens here in one place.
    pub fn apply_auth(&mut self, sa: SessionAuth) {
        self.up_bps = sa.up_bps;
        self.down_bps = sa.down_bps;
        self.auth = Some(sa);
    }
}
