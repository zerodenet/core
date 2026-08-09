use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundConfig {
    pub tag: String,
    pub protocol: OutboundProtocolConfig,
    #[serde(default)]
    pub udp: UdpPolicyConfig,
}

impl OutboundConfig {
    pub fn tag(&self) -> &str {
        &self.tag
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum OutboundProtocolConfig {
    #[serde(rename = "direct")]
    Direct,
    #[serde(rename = "block")]
    Block,
    #[serde(rename = "socks5")]
    Socks5 {
        server: String,
        port: u16,
        #[serde(default)]
        username: Option<String>,
        #[serde(default)]
        password: Option<String>,
    },
    #[serde(rename = "vless")]
    Vless {
        server: String,
        port: u16,
        id: String,
        #[serde(default)]
        flow: Option<String>,
        #[serde(default)]
        mux_concurrency: Option<u32>,
        #[serde(default)]
        xudp_concurrency: Option<u32>,
        #[serde(default)]
        mux_idle_timeout_secs: Option<u64>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
        #[serde(default)]
        tls: Option<Box<ClientTlsConfig>>,
        #[serde(default)]
        reality: Option<Box<RealityConfig>>,
        #[serde(default)]
        ws: Option<Box<WebSocketConfig>>,
        #[serde(default)]
        grpc: Option<Box<GrpcConfig>>,
        #[serde(default)]
        h2: Option<Box<H2Config>>,
        #[serde(default)]
        http_upgrade: Option<Box<HttpUpgradeConfig>>,
        #[serde(default)]
        split_http: Option<Box<SplitHttpConfig>>,
        #[serde(default)]
        quic: Option<Box<QuicConfig>>,
    },
    #[serde(rename = "hysteria2")]
    Hysteria2 {
        server: String,
        port: u16,
        password: String,
        #[serde(default)]
        insecure: bool,
        #[serde(default)]
        client_fingerprint: Option<String>,
    },
    #[serde(rename = "shadowsocks")]
    Shadowsocks {
        server: String,
        port: u16,
        password: String,
        #[serde(default = "default_ss_cipher")]
        cipher: String,
    },
    #[serde(rename = "trojan")]
    Trojan {
        server: String,
        port: u16,
        password: String,
        #[serde(default)]
        sni: Option<String>,
        #[serde(default)]
        insecure: bool,
        #[serde(default)]
        client_fingerprint: Option<String>,
        #[serde(default)]
        mux_concurrency: Option<u32>,
        #[serde(default)]
        mux_idle_timeout_secs: Option<u64>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
    },
    #[serde(rename = "vmess")]
    Vmess {
        server: String,
        port: u16,
        id: String,
        #[serde(default = "default_vmess_cipher")]
        cipher: String,
        #[serde(default)]
        mux_concurrency: Option<u32>,
        #[serde(default)]
        mux_idle_timeout_secs: Option<u64>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
        #[serde(default)]
        tls: Option<Box<ClientTlsConfig>>,
        #[serde(default)]
        ws: Option<Box<WebSocketConfig>>,
        #[serde(default)]
        grpc: Option<Box<GrpcConfig>>,
    },
    #[serde(rename = "mieru")]
    Mieru {
        server: String,
        port: u16,
        #[serde(default)]
        username: Option<String>,
        password: String,
    },
}

impl OutboundProtocolConfig {
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Block => "block",
            Self::Socks5 { .. } => "socks5",
            Self::Vless { .. } => "vless",
            Self::Hysteria2 { .. } => "hysteria2",
            Self::Shadowsocks { .. } => "shadowsocks",
            Self::Trojan { .. } => "trojan",
            Self::Vmess { .. } => "vmess",
            Self::Mieru { .. } => "mieru",
        }
    }

    pub fn runtime_kind(&self) -> OutboundRuntimeKind {
        match self {
            Self::Direct => OutboundRuntimeKind::Direct,
            Self::Block => OutboundRuntimeKind::Block,
            _ => OutboundRuntimeKind::Proxy,
        }
    }

    pub fn endpoint(&self) -> Option<(&str, u16)> {
        match self {
            Self::Socks5 { server, port, .. }
            | Self::Vless { server, port, .. }
            | Self::Hysteria2 { server, port, .. }
            | Self::Shadowsocks { server, port, .. }
            | Self::Trojan { server, port, .. }
            | Self::Vmess { server, port, .. }
            | Self::Mieru { server, port, .. } => Some((server, *port)),
            Self::Direct | Self::Block => None,
        }
    }

    /// Authentication contract declared by this outbound protocol.
    pub fn auth_requirement(&self) -> crate::auth::AuthRequirement {
        use crate::auth::AuthRequirement::*;
        match self {
            Self::Socks5 { .. } | Self::Mieru { .. } => UsernamePassword,
            Self::Hysteria2 { .. } | Self::Shadowsocks { .. } | Self::Trojan { .. } => PasswordOnly,
            Self::Direct | Self::Block => None,
            _ => Other,
        }
    }

    pub(super) fn normalize(&mut self) {
        match self {
            Self::Socks5 {
                username, password, ..
            } => {
                let resolved = crate::auth::resolve_username_password(
                    username.as_deref(),
                    password.as_deref(),
                );
                *username = resolved;
            }
            Self::Mieru {
                username, password, ..
            } => {
                let resolved = crate::auth::resolve_username_password(
                    username.as_deref(),
                    Some(password.as_str()),
                );
                *username = resolved;
            }
            Self::Vmess { cipher, .. } => {
                *cipher = normalize_vmess_cipher_name(cipher);
            }
            Self::Direct
            | Self::Block
            | Self::Vless { .. }
            | Self::Hysteria2 { .. }
            | Self::Shadowsocks { .. }
            | Self::Trojan { .. } => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundRuntimeKind {
    Direct,
    Block,
    Proxy,
}

pub(super) fn normalize_vmess_cipher_name(cipher: &str) -> String {
    match cipher {
        "auto" => "aes-128-gcm".to_owned(),
        _ => cipher.to_owned(),
    }
}

fn default_vmess_cipher() -> String {
    "aes-128-gcm".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmessUserConfig {
    pub id: String,
    #[serde(default = "default_vmess_cipher")]
    pub cipher: String,
    #[serde(default)]
    pub principal_key: Option<String>,
    #[serde(default)]
    pub up_bps: Option<u64>,
    #[serde(default)]
    pub down_bps: Option<u64>,
    #[serde(default)]
    pub device_limit: Option<u32>,
    #[serde(default)]
    pub quota_remaining_bytes: Option<u64>,
    #[serde(default)]
    pub policy_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundGroupConfig {
    pub tag: String,
    #[serde(flatten)]
    pub group: OutboundGroupKind,
}

impl OutboundGroupConfig {
    pub fn tag(&self) -> &str {
        &self.tag
    }

    pub fn active_outbound(&self) -> Option<&str> {
        match &self.group {
            OutboundGroupKind::Selector {
                outbounds,
                selected,
                default,
            } => selected
                .as_deref()
                .or(default.as_deref())
                .or_else(|| outbounds.first().map(String::as_str)),
            OutboundGroupKind::Fallback { outbounds } => outbounds.first().map(String::as_str),
            OutboundGroupKind::UrlTest { outbounds, .. } => outbounds.first().map(String::as_str),
            OutboundGroupKind::Relay { proxies } => proxies.first().map(String::as_str),
            OutboundGroupKind::LoadBalance {
                outbounds, default, ..
            } => default
                .as_deref()
                .or_else(|| outbounds.first().map(String::as_str)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    #[default]
    RoundRobin,
    Random,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum OutboundGroupKind {
    #[serde(rename = "selector")]
    Selector {
        outbounds: Vec<String>,
        #[serde(default)]
        default: Option<String>,
        #[serde(default)]
        selected: Option<String>,
    },
    #[serde(rename = "fallback")]
    Fallback { outbounds: Vec<String> },
    #[serde(rename = "url_test")]
    UrlTest {
        outbounds: Vec<String>,
        #[serde(default)]
        url: Option<String>,
        #[serde(default = "default_urltest_interval_seconds")]
        interval_seconds: u64,
        /// Selection hysteresis in milliseconds. A healthy current member is
        /// kept unless another member is faster by more than this value.
        #[serde(default)]
        tolerance_ms: u64,
    },
    #[serde(rename = "relay")]
    Relay { proxies: Vec<String> },
    #[serde(rename = "load_balance")]
    LoadBalance {
        outbounds: Vec<String>,
        #[serde(default)]
        default: Option<String>,
        #[serde(default)]
        strategy: LoadBalanceStrategy,
    },
}

impl OutboundGroupKind {
    pub fn members(&self) -> &[String] {
        match self {
            Self::Selector { outbounds, .. }
            | Self::Fallback { outbounds }
            | Self::UrlTest { outbounds, .. }
            | Self::LoadBalance { outbounds, .. } => outbounds,
            Self::Relay { proxies } => proxies,
        }
    }
}

const fn default_urltest_interval_seconds() -> u64 {
    300
}
