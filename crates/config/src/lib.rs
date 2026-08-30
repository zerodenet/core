pub mod auth;
mod compile;
mod error;
mod model;
mod rule_sets;
mod validate;

pub use auth::AuthRequirement;
pub use error::ConfigError;
pub use model::{
    ApiConfig, ClientTlsConfig, ControlApiConfig, ControlGrpcConfig, ControlGrpcTlsConfig,
    DnsAddressFamilyPolicy, DnsAnswerConfig, DnsCacheConfig, DnsConfig, DnsDispatchRuleConfig,
    DnsPolicyConfig, DnsReverseMappingConfig, DnsServerConfig, EventDispatcherConfig,
    EventSinkConfig, ExhaustedDeliveryPolicy, FakeIpConfigRef, FallbackConfig, GrpcConfig,
    H2Config, HookConfig, HttpUpgradeConfig, Hysteria2UserConfig, InboundConfig,
    InboundProtocolConfig, InboundRealityConfig, ListenConfig, LoadBalanceStrategy, LogConfig,
    LogFileConfig, LogRateLimit, MieruUserConfig, ModeConfig, NetworkOptionsConfig, OutboundConfig,
    OutboundGroupConfig, OutboundGroupKind, OutboundProtocolConfig, OutboundRuntimeKind,
    QuicConfig, RealityConfig, RouteActionConfig, RouteConfig, RouteRuleConfig, RouteRuleSetConfig,
    RuleConditionConfig, RuleSetConfig, RuleSetFormatConfig, RuleSetSourceType, RuntimeConfig,
    RuntimeOptionsConfig, ShadowsocksUserConfig, Socks5UserConfig, SplitHttpConfig, TlsConfig,
    TrojanUserConfig, TunConfig, UrlRewriteRule, VlessUserConfig, VmessUserConfig, WebSocketConfig,
    DEFAULT_EVENT_LOG_CAPACITY, DEFAULT_LATENCY_TEST_URL,
};
pub use zero_api::CONFIG_SCHEMA_VERSION;
