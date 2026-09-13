mod browser_dialer;
mod tls;
pub use browser_dialer::BrowserDialerConfig;
use serde::{Deserialize, Serialize};
pub use tls::{
    ClientTlsOptionsConfig, EchForceQueryConfig, ServerTlsOptionsConfig, TlsBackendConfig,
    TlsCertificateFilesConfig, TlsCertificateUsageConfig, TlsParametersConfig,
};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    InboundFallbackProfile, ServerTlsProfile, WebSocketTransportProfile,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub options: ServerTlsOptionsConfig,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
    #[serde(default)]
    pub alpn: Vec<String>,
    /// TLS server fingerprint preset: "chrome", "firefox", "safari",
    /// "ios", "edge", "randomized", or empty/"none" for rustls defaults.
    /// Controls cipher suite preference order in the ServerHello.
    #[serde(default)]
    pub server_fingerprint: Option<String>,
}

impl ServerTlsProfile for TlsConfig {
    fn tls_options(&self) -> zero_traits::ServerTlsOptions {
        self.options.to_options()
    }
    fn cert_path(&self) -> &str {
        &self.cert_path
    }

    fn key_path(&self) -> &str {
        &self.key_path
    }

    fn alpn(&self) -> &[String] {
        self.alpn.as_slice()
    }

    fn server_fingerprint(&self) -> Option<&str> {
        self.server_fingerprint.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTlsConfig {
    #[serde(default)]
    pub options: ClientTlsOptionsConfig,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub disable_sni: bool,
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub insecure: bool,
    #[serde(default)]
    pub alpn: Vec<String>,
    /// TLS client fingerprint preset: "chrome", "firefox", "safari",
    /// "ios", "edge", "randomized", or empty/"none" for rustls defaults.
    #[serde(default)]
    pub client_fingerprint: Option<String>,
}

impl ClientTlsProfile for ClientTlsConfig {
    fn tls_options(&self) -> zero_traits::ClientTlsOptions {
        self.options.to_options()
    }
    fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }

    fn disable_sni(&self) -> bool {
        self.disable_sni
    }

    fn ca_cert_path(&self) -> Option<&str> {
        self.ca_cert_path.as_deref()
    }

    fn insecure(&self) -> bool {
        self.insecure
    }

    fn alpn(&self) -> &[String] {
        self.alpn.as_slice()
    }

    fn client_fingerprint(&self) -> Option<&str> {
        self.client_fingerprint.as_deref()
    }
}

mod reality;
pub use reality::{
    InboundRealityConfig, RealityConfig, RealityFallbackRateConfig, RealityTargetConfig,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    #[serde(default)]
    pub browser_dialer: Option<BrowserDialerConfig>,
    #[serde(default)]
    pub accept_proxy_protocol: bool,
    #[serde(default)]
    pub heartbeat_period_secs: u32,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_ws_path")]
    pub path: String,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

impl WebSocketTransportProfile for WebSocketConfig {
    fn browser_dialer(&self) -> Option<zero_traits::BrowserDialerSettings> {
        self.browser_dialer
            .as_ref()
            .map(BrowserDialerConfig::settings)
    }
    fn accept_proxy_protocol(&self) -> bool {
        self.accept_proxy_protocol
    }
    fn heartbeat_period_secs(&self) -> u32 {
        self.heartbeat_period_secs
    }
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }
    fn path(&self) -> &str {
        &self.path
    }

    fn header_pairs(&self) -> Vec<(String, String)> {
        self.headers
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

fn default_ws_path() -> String {
    "/".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcConfig {
    #[serde(deserialize_with = "deserialize_service_names")]
    pub service_names: Vec<String>,
    #[serde(default)]
    pub authority: Option<String>,
    #[serde(default)]
    pub multi_mode: bool,
    #[serde(default)]
    pub idle_timeout_secs: u32,
    #[serde(default)]
    pub health_check_timeout_secs: u32,
    #[serde(default)]
    pub permit_without_stream: bool,
    #[serde(default)]
    pub initial_window_size: u32,
    #[serde(default)]
    pub user_agent: Option<String>,
}

impl GrpcConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.initial_window_size > 0x7fff_ffff {
            return Err("gRPC initial window exceeds HTTP/2 limit");
        }
        if self.authority.as_ref().is_some_and(|value| {
            value
                .bytes()
                .any(|b| b <= 32 || b >= 127 || b"/?#@".contains(&b))
        }) {
            return Err("invalid gRPC authority");
        }
        if self
            .user_agent
            .as_ref()
            .is_some_and(|value| value.bytes().any(|b| (b < 32 && b != b'\t') || b == 127))
        {
            return Err("invalid gRPC user agent");
        }
        Ok(())
    }
}

impl GrpcTransportProfile for GrpcConfig {
    fn service_names(&self) -> &[String] {
        self.service_names.as_slice()
    }
    fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }
    fn multi_mode(&self) -> bool {
        self.multi_mode
    }
    fn idle_timeout_secs(&self) -> u32 {
        self.idle_timeout_secs
    }
    fn health_check_timeout_secs(&self) -> u32 {
        self.health_check_timeout_secs
    }
    fn permit_without_stream(&self) -> bool {
        self.permit_without_stream
    }
    fn initial_window_size(&self) -> u32 {
        self.initial_window_size
    }
    fn user_agent(&self) -> Option<&str> {
        self.user_agent.as_deref()
    }
}

fn deserialize_service_names<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, SeqAccess, Visitor};
    use std::fmt;

    struct ServiceNames;

    impl<'de> Visitor<'de> for ServiceNames {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or array of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(vec![value.to_owned()])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut names = Vec::new();
            while let Some(name) = seq.next_element::<String>()? {
                names.push(name);
            }
            if names.is_empty() {
                return Err(de::Error::invalid_length(0, &self));
            }
            Ok(names)
        }
    }

    deserializer.deserialize_any(ServiceNames)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H2Config {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_h2_path")]
    pub path: String,
}

impl H2TransportProfile for H2Config {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }
}

fn default_h2_path() -> String {
    "/".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpUpgradeConfig {
    #[serde(default)]
    pub accept_proxy_protocol: bool,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_http_upgrade_path")]
    pub path: String,
}

impl HttpUpgradeTransportProfile for HttpUpgradeConfig {
    fn accept_proxy_protocol(&self) -> bool {
        self.accept_proxy_protocol
    }
    fn header_pairs(&self) -> Vec<(String, String)> {
        self.headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }
}

fn default_http_upgrade_path() -> String {
    "/".to_string()
}

mod split_http;
pub use split_http::{
    SplitHttpConfig, SplitHttpDownloadConfig, SplitHttpRangeConfig, SplitHttpXmuxConfig,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackConfig {
    #[serde(default)]
    pub server: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub alpn: Option<String>,
    #[serde(default)]
    pub rules: Vec<FallbackRuleConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackRuleConfig {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub alpn: String,
    #[serde(default)]
    pub path: String,
    pub destination: FallbackDestinationConfig,
    #[serde(default)]
    pub proxy_protocol: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FallbackDestinationConfig {
    Tcp { server: String, port: u16 },
    Unix { path: String },
}

impl InboundFallbackProfile for FallbackConfig {
    fn server(&self) -> &str {
        &self.server
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn alpn(&self) -> Option<&str> {
        self.alpn.as_deref()
    }
    fn rules(&self) -> Vec<zero_traits::FallbackRule> {
        self.rules
            .iter()
            .map(|rule| zero_traits::FallbackRule {
                name: rule.name.clone(),
                alpn: rule.alpn.clone(),
                path: rule.path.clone(),
                endpoint: match &rule.destination {
                    FallbackDestinationConfig::Tcp { server, port } => {
                        zero_traits::FallbackEndpoint::Tcp {
                            server: server.clone(),
                            port: *port,
                        }
                    }
                    FallbackDestinationConfig::Unix { path } => {
                        zero_traits::FallbackEndpoint::Unix { path: path.clone() }
                    }
                },
                proxy_protocol: rule.proxy_protocol,
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuicConfig {
    #[serde(default)]
    pub client_options: ClientTlsOptionsConfig,
    #[serde(default)]
    pub server_options: ServerTlsOptionsConfig,
    // Inbound
    #[serde(default)]
    pub cert_path: Option<String>,
    #[serde(default)]
    pub key_path: Option<String>,
    // Outbound
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    #[serde(default)]
    pub insecure: bool,
}

impl ClientTlsProfile for QuicConfig {
    fn tls_options(&self) -> zero_traits::ClientTlsOptions {
        self.client_options.to_options()
    }
    fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
    fn disable_sni(&self) -> bool {
        false
    }
    fn ca_cert_path(&self) -> Option<&str> {
        self.ca_cert_path.as_deref()
    }
    fn insecure(&self) -> bool {
        self.insecure
    }
    fn alpn(&self) -> &[String] {
        &[]
    }
    fn client_fingerprint(&self) -> Option<&str> {
        None
    }
}
impl ServerTlsProfile for QuicConfig {
    fn tls_options(&self) -> zero_traits::ServerTlsOptions {
        self.server_options.to_options()
    }
    fn cert_path(&self) -> &str {
        self.cert_path.as_deref().unwrap_or("")
    }
    fn key_path(&self) -> &str {
        self.key_path.as_deref().unwrap_or("")
    }
    fn alpn(&self) -> &[String] {
        &[]
    }
    fn server_fingerprint(&self) -> Option<&str> {
        None
    }
}
