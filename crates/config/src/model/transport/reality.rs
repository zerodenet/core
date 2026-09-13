use super::FallbackDestinationConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboundRealityConfig {
    #[serde(default)]
    pub target: Option<RealityTargetConfig>,
    #[serde(default)]
    pub server_names: Vec<String>,
    #[serde(default)]
    pub min_client_version: Option<String>,
    #[serde(default)]
    pub max_client_version: Option<String>,
    #[serde(default)]
    pub max_time_diff_ms: u64,
    #[serde(default)]
    pub mldsa65_seed: Option<String>,
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
    #[serde(default)]
    pub spider_x: String,
    #[serde(default = "default_hybrid")]
    pub hybrid_key_exchange: bool,
    #[serde(default)]
    pub mldsa65_verify: Option<String>,
    #[serde(alias = "password")]
    pub public_key: String,
    #[serde(default)]
    pub short_id: String,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub cipher_suites: Vec<String>,
    /// Versioned ztls ClientHello template. Short aliases currently pin to
    /// chrome-133, firefox-148, safari-26.3, edge-85, ios-14 and qq-11.1.
    /// Explicit versioned names and random/randomized/randomizednoalpn are accepted.
    #[serde(default = "default_reality_client_fingerprint")]
    pub client_fingerprint: String,
}

fn default_reality_client_fingerprint() -> String {
    "chrome".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealityTargetConfig {
    pub destination: FallbackDestinationConfig,
    #[serde(default)]
    pub proxy_protocol: u8,
    #[serde(default)]
    pub upload: RealityFallbackRateConfig,
    #[serde(default)]
    pub download: RealityFallbackRateConfig,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RealityFallbackRateConfig {
    #[serde(default)]
    pub after_bytes: u64,
    #[serde(default)]
    pub bytes_per_sec: u64,
    #[serde(default)]
    pub burst_bytes: u64,
}

fn default_hybrid() -> bool {
    true
}
