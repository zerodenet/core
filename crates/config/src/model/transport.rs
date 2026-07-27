use serde::{Deserialize, Serialize};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, H2TransportProfile, HttpUpgradeTransportProfile,
    InboundFallbackProfile, ServerTlsProfile, SplitHttpTransportProfile, WebSocketTransportProfile,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub cert_path: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboundRealityConfig {
    pub private_key: String,
    #[serde(default)]
    pub short_ids: Vec<String>,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub cipher_suites: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealityConfig {
    pub public_key: String,
    #[serde(default)]
    pub short_id: String,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub cipher_suites: Vec<String>,
    /// Versioned ztls ClientHello template. Short aliases currently pin to
    /// chrome-120, firefox-120, safari-16.0, or edge-120.
    #[serde(default = "default_reality_client_fingerprint")]
    pub client_fingerprint: String,
}

fn default_reality_client_fingerprint() -> String {
    "chrome".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    #[serde(default = "default_ws_path")]
    pub path: String,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

impl WebSocketTransportProfile for WebSocketConfig {
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
}

impl GrpcTransportProfile for GrpcConfig {
    fn service_names(&self) -> &[String] {
        self.service_names.as_slice()
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
    pub host: Option<String>,
    #[serde(default = "default_http_upgrade_path")]
    pub path: String,
}

impl HttpUpgradeTransportProfile for HttpUpgradeConfig {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitHttpConfig {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_split_http_path")]
    pub path: String,
    /// XHTTP framing mode.
    ///
    /// - `auto` (default) → `stream-one`: single bidirectional connection,
    ///   usable as a relay-chain final hop.
    /// - `stream-one` → explicit single connection.
    /// - `packet-up` / `stream-up` → legacy two-connection model (POST upload
    ///   + GET download), single-hop direct only — cannot be a relay final hop.
    ///
    /// XTLS removed the standalone `quic` transport; XHTTP `stream-one` over
    /// H3 is its successor. This project implements the client (outbound)
    /// side; `auto` and `stream-one` resolve to the single-connection path.
    #[serde(default = "default_xhttp_mode")]
    pub mode: String,
}

impl SplitHttpTransportProfile for SplitHttpConfig {
    fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn mode(&self) -> &str {
        &self.mode
    }
}

fn default_split_http_path() -> String {
    "/".to_string()
}

fn default_xhttp_mode() -> String {
    "auto".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackConfig {
    pub server: String,
    pub port: u16,
    #[serde(default)]
    pub alpn: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuicConfig {
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
