use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("unsupported config schema version {found}; this core supports version {supported}")]
    UnsupportedSchemaVersion { found: u32, supported: u32 },
    #[error("failed to read config `{path}`: {source}")]
    ReadConfig {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read rule set `{path}`: {source}")]
    ReadRuleSet {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    ParseConfig(#[from] serde_json::Error),
    #[error("`{scope}` tag must not be empty")]
    EmptyTag { scope: &'static str },
    #[error("duplicate `{scope}` tag `{tag}`")]
    DuplicateTag { scope: &'static str, tag: String },
    #[error(
        "duplicate inbound listen endpoint `{address}:{port}`; use `mixed` for multi-protocol same-port listening"
    )]
    DuplicateInboundListen { address: String, port: u16 },
    #[error("invalid rule condition: {0}")]
    InvalidRuleCondition(String),
    #[error("invalid inbound: {0}")]
    InvalidInbound(String),
    #[error("invalid outbound: {0}")]
    InvalidOutbound(String),
    #[error("invalid rule set: {0}")]
    InvalidRuleSet(String),
    #[error("invalid route action: {0}")]
    InvalidRouteAction(String),
    #[error("invalid outbound group: {0}")]
    InvalidOutboundGroup(String),
    #[error("invalid runtime config: {0}")]
    InvalidRuntime(String),
    #[error("invalid api config: {0}")]
    InvalidApi(String),
    #[error("invalid mode config: {0}")]
    InvalidMode(String),
    #[error("duplicate route target tag `{tag}` across outbounds and outbound groups")]
    DuplicateRouteTargetTag { tag: String },
    #[error("route or mode references undefined target tag `{tag}`")]
    UndefinedRouteTargetTag { tag: String },
    #[error("route references undefined rule set tag `{tag}`")]
    UndefinedRuleSetTag { tag: String },
    #[error("invalid dns config: {0}")]
    InvalidDns(String),
}
