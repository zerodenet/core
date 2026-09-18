pub mod auth;
mod compile;
mod error;
mod model;
mod rule_sets;
mod validate;

pub use auth::AuthRequirement;
pub use error::ConfigError;
pub use mieru_config::{MieruTrafficPatternConfig, MieruTransportOptions};
pub use model::{
    ApiConfig, BrowserDialerConfig, ClientTlsConfig, ControlApiConfig, ControlGrpcConfig,
    ControlGrpcTlsConfig, DnsAddressFamilyPolicy, DnsAnswerConfig, DnsCacheConfig, DnsConfig,
    DnsDispatchRuleConfig, DnsPolicyConfig, DnsReverseMappingConfig, DnsServerConfig,
    EventDispatcherConfig, EventSinkConfig, ExhaustedDeliveryPolicy, FakeIpConfigRef,
    FallbackConfig, FallbackDestinationConfig, FallbackRuleConfig, FinalMaskConfig, GrpcConfig,
    H2Config, HookConfig, HttpUpgradeConfig, Hysteria2CongestionConfig, Hysteria2MasqueradeConfig,
    Hysteria2MasqueradeResponseConfig, Hysteria2ObfsConfig, Hysteria2QuicConfig,
    Hysteria2TransportConfig, Hysteria2UserConfig, HysteriaCarrierMasqueradeConfig,
    HysteriaTransportConfig, InboundConfig, InboundProtocolConfig, InboundRealityConfig,
    ListenConfig, LoadBalanceStrategy, LogConfig, LogFileConfig, LogRateLimit, MaskItemConfig,
    MaskRangeConfig, MieruTransport, MieruUserConfig, MkcpConfig, ModeConfig, NetworkOptionsConfig,
    NoiseItemConfig, OutboundConfig, OutboundGroupConfig, OutboundGroupKind,
    OutboundProtocolConfig, OutboundRuntimeKind, QuicConfig, QuicParametersConfig, RealityConfig,
    RealityFallbackRateConfig, RealityTargetConfig, ReverseSniffingConfig, RouteActionConfig,
    RouteConfig, RouteRuleConfig, RouteRuleSetConfig, RuleConditionConfig, RuleSetConfig,
    RuleSetFormatConfig, RuleSetSourceType, RuntimeConfig, RuntimeOptionsConfig,
    ShadowsocksUserConfig, Socks5UserConfig, SplitHttpConfig, SplitHttpDownloadConfig,
    SplitHttpRangeConfig, SplitHttpXmuxConfig, TcpMaskConfig, TcpMaskItemConfig, TlsConfig,
    TrojanUserConfig, TunConfig, UdpHopConfig, UdpMaskConfig, UrlRewriteRule, VlessUserConfig,
    VmessUserConfig, WebSocketConfig, DEFAULT_EVENT_LOG_CAPACITY, DEFAULT_LATENCY_TEST_URL,
};
pub use zero_api::CONFIG_SCHEMA_VERSION;

pub use model::{
    ClientTlsOptionsConfig, EchForceQueryConfig, ServerTlsOptionsConfig, TlsBackendConfig,
    TlsCertificateFilesConfig, TlsCertificateUsageConfig, TlsParametersConfig,
};
