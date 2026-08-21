use ipnet::IpNet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    #[serde(default)]
    pub rules: Vec<RouteRuleConfig>,
    #[serde(rename = "final")]
    pub final_action: RouteActionConfig,
    /// Path to a GeoLite2-Country.mmdb file for the `geoip` condition.
    #[serde(default)]
    pub geoip_database: Option<String>,
    /// URL rewrite rules applied before routing.
    #[serde(default)]
    pub url_rewrite: Vec<UrlRewriteRule>,
}

/// Domain rewrite rule: `from` or `from_regex` ->`to`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UrlRewriteRule {
    /// Exact domain to match.
    #[serde(default)]
    pub from: Option<String>,
    /// Regex pattern to match against the domain.
    #[serde(default)]
    pub from_regex: Option<String>,
    /// Replacement domain.  Supports `$1`, `$2`, etc. for regex captures.
    pub to: String,
    /// If set, return an HTTP redirect response (e.g. 302) instead of
    /// silently rewriting the target.  Only meaningful for HTTP-based
    /// protocols; SOCKS5 etc. ignore this and silently rewrite.
    #[serde(default)]
    pub status_code: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSetConfig {
    #[serde(rename = "type")]
    pub source_type: RuleSetSourceType,
    /// Path to a local file, or fallback cache path for URL sources.
    pub path: String,
    /// URL to fetch the rule set from (required when type = "url").
    #[serde(default)]
    pub url: Option<String>,
    /// Re-fetch interval in seconds (default 86400 = 24h).
    #[serde(default = "default_rule_set_update_interval")]
    pub update_interval_seconds: u64,
    pub format: RuleSetFormatConfig,
}

impl RuleSetConfig {
    pub fn source_path(&self) -> &str {
        &self.path
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleSetSourceType {
    #[serde(rename = "file")]
    File,
    #[serde(rename = "url")]
    Url,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleSetFormatConfig {
    #[serde(rename = "domain_list")]
    DomainList,
    #[serde(rename = "cidr_list")]
    CidrList,
    #[serde(rename = "zero_rule_ir", alias = "zero_ir")]
    ZeroRuleIr,
    #[serde(rename = "zrs")]
    Zrs,
}

fn default_rule_set_update_interval() -> u64 {
    86400
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteRuleConfig {
    pub condition: RuleConditionConfig,
    pub action: RouteActionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RuleConditionConfig {
    #[serde(rename = "inbound")]
    Inbound { values: Vec<String> },
    #[serde(rename = "domain")]
    Domain { values: Vec<String> },
    #[serde(rename = "domain_keyword")]
    DomainKeyword { values: Vec<String> },
    #[serde(rename = "domain_regex")]
    DomainRegex { values: Vec<String> },
    #[serde(rename = "ip")]
    Ip { values: Vec<IpNet> },
    #[serde(rename = "rule_set")]
    RuleSet { tag: String },
    #[serde(rename = "geoip")]
    GeoIp { values: Vec<String> },
    #[serde(rename = "sni")]
    Sni { values: Vec<String> },
    #[serde(rename = "and")]
    And { items: Vec<RuleConditionConfig> },
    #[serde(rename = "or")]
    Or { items: Vec<RuleConditionConfig> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RouteActionConfig {
    #[serde(rename = "direct")]
    Direct,
    #[serde(rename = "reject")]
    Reject,
    #[serde(rename = "route")]
    Route { outbound: String },
}
