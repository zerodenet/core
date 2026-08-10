use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboundConfig {
    pub tag: String,
    pub listen: ListenConfig,
    pub protocol: InboundProtocolConfig,
    #[serde(default)]
    pub udp: UdpPolicyConfig,
    /// TCP idle timeout in seconds.  Kernel default is 300 (5 min).
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenConfig {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum InboundProtocolConfig {
    #[serde(rename = "socks5")]
    Socks5 {
        #[serde(default)]
        users: Vec<Socks5UserConfig>,
    },
    #[serde(rename = "http")]
    HttpConnect,
    #[serde(rename = "mixed")]
    Mixed {
        #[serde(default)]
        socks5_users: Vec<Socks5UserConfig>,
    },
    #[serde(rename = "vless")]
    Vless {
        users: Vec<VlessUserConfig>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
        #[serde(default)]
        tls: Option<Box<TlsConfig>>,
        #[serde(default)]
        reality: Option<Box<InboundRealityConfig>>,
        #[serde(default)]
        ws: Option<Box<WebSocketConfig>>,
        #[serde(default)]
        grpc: Option<Box<GrpcConfig>>,
        #[serde(default)]
        h2: Option<Box<H2Config>>,
        #[serde(default)]
        http_upgrade: Option<Box<HttpUpgradeConfig>>,
        #[serde(default)]
        fallback: Option<Box<FallbackConfig>>,
        #[serde(default)]
        quic: Option<Box<QuicConfig>>,
        #[serde(default)]
        split_http: Option<Box<SplitHttpConfig>>,
    },
    #[serde(rename = "hysteria2")]
    Hysteria2 {
        #[serde(default)]
        password: String,
        #[serde(default)]
        users: Vec<Hysteria2UserConfig>,
        #[serde(default)]
        cert_path: Option<String>,
        #[serde(default)]
        key_path: Option<String>,
        #[serde(default)]
        up_bps: Option<u64>,
        #[serde(default)]
        down_bps: Option<u64>,
    },
    #[serde(rename = "shadowsocks")]
    Shadowsocks {
        #[serde(default)]
        password: String,
        /// Static SIP023 server identity PSK for managed 2022 AES credentials.
        #[serde(default)]
        identity_password: Option<String>,
        #[serde(default)]
        users: Vec<ShadowsocksUserConfig>,
        #[serde(default = "default_ss_cipher")]
        cipher: String,
        #[serde(default)]
        up_bps: Option<u64>,
        #[serde(default)]
        down_bps: Option<u64>,
    },
    #[serde(rename = "trojan")]
    Trojan {
        #[serde(default)]
        password: String,
        #[serde(default)]
        users: Vec<TrojanUserConfig>,
        #[serde(default)]
        sni: Option<String>,
        #[serde(default)]
        tls: Option<TlsConfig>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
        #[serde(default)]
        up_bps: Option<u64>,
        #[serde(default)]
        down_bps: Option<u64>,
    },
    #[serde(rename = "vmess")]
    Vmess {
        users: Vec<VmessUserConfig>,
        #[serde(default)]
        mux_response_backlog_frames: Option<u32>,
        #[serde(default)]
        mux_response_backlog_bytes: Option<u64>,
        #[serde(default)]
        tls: Option<Box<TlsConfig>>,
        #[serde(default)]
        ws: Option<Box<WebSocketConfig>>,
        #[serde(default)]
        grpc: Option<Box<GrpcConfig>>,
    },
    #[serde(rename = "direct")]
    Direct {
        #[serde(default)]
        target: Option<String>,
        #[serde(default)]
        port: Option<u16>,
    },
    #[serde(rename = "mieru")]
    Mieru { users: Vec<MieruUserConfig> },
}

impl InboundProtocolConfig {
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Socks5 { .. } => "socks5",
            Self::HttpConnect => "http",
            Self::Mixed { .. } => "mixed",
            Self::Vless { .. } => "vless",
            Self::Hysteria2 { .. } => "hysteria2",
            Self::Shadowsocks { .. } => "shadowsocks",
            Self::Trojan { .. } => "trojan",
            Self::Vmess { .. } => "vmess",
            Self::Direct { .. } => "direct",
            Self::Mieru { .. } => "mieru",
        }
    }

    pub fn tls_config(&self) -> Option<&TlsConfig> {
        match self {
            Self::Vless { tls, .. } | Self::Vmess { tls, .. } => tls.as_deref(),
            Self::Trojan { tls, .. } => tls.as_ref(),
            _ => None,
        }
    }

    /// Global (per-inbound) rate limits. Returns `(up_bps, down_bps)`.
    /// Per-user limits are handled separately by protocol accept handlers.
    pub fn rate_limits(&self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Trojan {
                up_bps, down_bps, ..
            }
            | Self::Shadowsocks {
                up_bps, down_bps, ..
            }
            | Self::Hysteria2 {
                up_bps, down_bps, ..
            } => (*up_bps, *down_bps),
            _ => (None, None),
        }
    }

    /// Runtime-neutral principal policy facts declared by authenticated entries.
    ///
    /// Consumers such as the engine use this surface instead of matching on
    /// concrete protocol variants. Credentials without a complete policy identity
    /// are intentionally omitted because the runtime cannot associate them with
    /// a stable principal policy.
    pub fn principal_policy_revisions(&self) -> Vec<(&str, u64)> {
        match self {
            Self::Vless { users, .. } => users
                .iter()
                .filter_map(|user| Some((user.principal_key.as_deref()?, user.policy_revision?)))
                .collect(),
            Self::Vmess { users, .. } => users
                .iter()
                .filter_map(|user| Some((user.principal_key.as_deref()?, user.policy_revision?)))
                .collect(),
            Self::Trojan { users, .. } => users
                .iter()
                .filter_map(|user| Some((user.principal_key.as_deref()?, user.policy_revision?)))
                .collect(),
            Self::Shadowsocks { users, .. } => users
                .iter()
                .filter_map(|user| Some((user.principal_key.as_deref()?, user.policy_revision?)))
                .collect(),
            Self::Hysteria2 { users, .. } => users
                .iter()
                .filter_map(|user| Some((user.principal_key.as_deref()?, user.policy_revision?)))
                .collect(),
            _ => Vec::new(),
        }
    }
}

pub(super) fn default_ss_cipher() -> String {
    "chacha20-ietf-poly1305".to_string()
}

impl InboundProtocolConfig {
    pub fn socks5_users(&self) -> &[Socks5UserConfig] {
        match self {
            Self::Socks5 { users } => users,
            Self::Mixed { socks5_users } => socks5_users,
            Self::HttpConnect
            | Self::Direct { .. }
            | Self::Vless { .. }
            | Self::Hysteria2 { .. }
            | Self::Shadowsocks { .. }
            | Self::Trojan { .. }
            | Self::Vmess { .. }
            | Self::Mieru { .. } => &[],
        }
    }

    pub fn vless_users(&self) -> &[VlessUserConfig] {
        match self {
            Self::Vless { users, .. } => users,
            _ => &[],
        }
    }

    pub fn vless_tls(&self) -> Option<&TlsConfig> {
        match self {
            Self::Vless { tls, .. } => tls.as_deref(),
            _ => None,
        }
    }

    pub fn vless_reality(&self) -> Option<&InboundRealityConfig> {
        match self {
            Self::Vless { reality, .. } => reality.as_deref(),
            _ => None,
        }
    }

    pub fn vless_ws(&self) -> Option<&WebSocketConfig> {
        match self {
            Self::Vless { ws, .. } => ws.as_deref(),
            _ => None,
        }
    }

    pub fn vless_grpc(&self) -> Option<&GrpcConfig> {
        match self {
            Self::Vless { grpc, .. } => grpc.as_deref(),
            _ => None,
        }
    }

    pub fn vless_h2(&self) -> Option<&H2Config> {
        match self {
            Self::Vless { h2, .. } => h2.as_deref(),
            _ => None,
        }
    }

    pub fn vless_http_upgrade(&self) -> Option<&HttpUpgradeConfig> {
        match self {
            Self::Vless { http_upgrade, .. } => http_upgrade.as_deref(),
            _ => None,
        }
    }

    pub fn vless_split_http(&self) -> Option<&SplitHttpConfig> {
        match self {
            Self::Vless { split_http, .. } => split_http.as_deref(),
            _ => None,
        }
    }

    pub fn vless_fallback(&self) -> Option<&FallbackConfig> {
        match self {
            Self::Vless { fallback, .. } => fallback.as_deref(),
            _ => None,
        }
    }

    pub fn vless_quic(&self) -> Option<&QuicConfig> {
        match self {
            Self::Vless { quic, .. } => quic.as_deref(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MieruUserConfig {
    #[serde(default)]
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub principal_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Socks5UserConfig {
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub principal_key: Option<String>,
    #[serde(default)]
    pub up_bps: Option<u64>,
    #[serde(default)]
    pub down_bps: Option<u64>,
}

impl InboundProtocolConfig {
    /// Authentication contract declared by this inbound protocol.
    pub fn auth_requirement(&self) -> crate::auth::AuthRequirement {
        use crate::auth::AuthRequirement::*;
        match self {
            Self::Socks5 { .. } | Self::Mixed { .. } | Self::Mieru { .. } => UsernamePassword,
            Self::Hysteria2 { .. } | Self::Shadowsocks { .. } | Self::Trojan { .. } => PasswordOnly,
            Self::HttpConnect | Self::Direct { .. } => None,
            _ => Other,
        }
    }

    pub(super) fn normalize(&mut self) {
        match self {
            Self::Socks5 { users } => normalize_socks5_users(users),
            Self::Mixed { socks5_users } => normalize_socks5_users(socks5_users),
            Self::Mieru { users } => {
                for user in users {
                    if let Some(name) = crate::auth::resolve_username_password(
                        Some(&user.username),
                        Some(&user.password),
                    ) {
                        user.username = name;
                    }
                }
            }
            Self::Vmess { users, .. } => {
                for user in users {
                    user.cipher = normalize_vmess_cipher_name(&user.cipher);
                }
            }
            Self::HttpConnect
            | Self::Vless { .. }
            | Self::Hysteria2 { .. }
            | Self::Shadowsocks { .. }
            | Self::Trojan { .. }
            | Self::Direct { .. } => {}
        }
    }
}

fn normalize_socks5_users(users: &mut Vec<Socks5UserConfig>) {
    users.retain_mut(|user| {
        match crate::auth::resolve_username_password(Some(&user.username), Some(&user.password)) {
            Some(name) => {
                user.username = name;
                true
            }
            None => false,
        }
    });
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VlessUserConfig {
    pub id: String,
    #[serde(default)]
    pub flow: Option<String>,
    #[serde(default)]
    pub principal_key: Option<String>,
    #[serde(default)]
    pub up_bps: Option<u64>,
    #[serde(default)]
    pub down_bps: Option<u64>,
    /// Maximum concurrent unique source IP addresses for this principal.
    /// The runtime enforces this across TCP, UDP, MUX, and QUIC carriers.
    #[serde(default)]
    pub device_limit: Option<u32>,
    #[serde(default)]
    pub quota_remaining_bytes: Option<u64>,
    /// Principal policy revision that owns `quota_remaining_bytes`.
    #[serde(default)]
    pub policy_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrojanUserConfig {
    pub password: String,
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
#[serde(deny_unknown_fields)]
pub struct ShadowsocksUserConfig {
    pub password: String,
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
#[serde(deny_unknown_fields)]
pub struct Hysteria2UserConfig {
    pub password: String,
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
