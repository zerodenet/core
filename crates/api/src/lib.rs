pub mod auth;
pub mod capabilities;
pub mod command;
pub mod error;
pub mod event;
pub mod flow;
pub mod query;
pub mod response;
pub mod sink;
pub mod snapshot;
pub mod traits;

pub use auth::{AuthContext, Permission};
pub use capabilities::{
    AdapterCapability, ApiCapabilities, ApiContractVersions, CapabilityState, ContractVersionRange,
    ProtocolCapability, ProtocolNetworkCapability, SinkCapability,
};
pub use command::{
    CommandRequest, CommandResponse, ConfigApplyCommand, ConfigValidateCommand,
    DiagnosticsDnsCacheCommand, DiagnosticsDnsLookupCommand, DiagnosticsFakeipLookupCommand,
    DiagnosticsProbeOutboundCommand, DiagnosticsProbeTargetCommand, DiagnosticsTraceRouteCommand,
    FakeIpClearCommand, FlowCloseCommand, ModeSetCommand, PolicyProbeCommand, PolicySelectCommand,
    TunStartCommand, TunStopCommand,
};
pub use error::{ApiError, ApiErrorCode, ErrorDetail};
pub use event::{
    event_type, ApiEvent, EventFilter, EventReplay, PassiveRelayHealthChangedPayload,
    PassiveRelayHealthState, PublishResult,
};
pub use flow::{
    AuthInfo, EndpointRef, FlowAddressFamilyFallback, FlowConnectionAttempt, FlowEgressContext,
    FlowEventPayload, FlowFailureInfo, FlowNetworkContext, FlowNetworkInterface, FlowOutcome,
    FlowPath, FlowRecord, FlowRecordTiming, FlowResult, FlowRoute, FlowRouteLookup,
    FlowSocketBinding, FlowSource, FlowState, FlowTarget, FlowThroughput, FlowTiming,
    MatchedRuleInfo, Network, PolicyDecision, PolicyProbeCompletedPayload, PolicyProbeMember,
    PolicySelectedPayload, RouteDecision, TargetAddress, TrafficStats, WarningPayload,
};
pub use query::{
    CapabilitiesQuery, ConfigQuery, DiagnosticsQuery, FlowFilter, FlowGetQuery, FlowListQuery,
    HealthQuery, HealthSnapshot, PoliciesQuery, PolicyGetQuery, PrincipalFlowsQuery, QueryRequest,
    QueryResponse, RuntimeQuery, SinkStatusSnapshot, SinksQuery, StatsQuery,
    TunFamilyEgressAvailability, TunFamilyEgressSnapshot, TunStatusQuery, TunStatusSnapshot,
};
pub use response::{ApiResponse, EnvelopeError, RawResponse};
pub use sink::{
    CallbackEventSink, DeadLetterSink, JsonLineEventSink, MemorySink, OutboxCorruptionClass,
    OutboxRecoveryState, OutboxRecoveryStatus, OutboxStorageStatus, RotatingFileSink,
    SinkDeliveryStatus, SinkManager, SinkStatus,
};
pub use snapshot::{
    AddressSnapshot, AuthSnapshot, CompletedFlowSnapshot, ConfigSnapshot, FlowSnapshot,
    ListenerSnapshot, ModeSnapshot, OutboundTargetSnapshot, OutboundTrafficStats,
    PolicyMemberSnapshot, PolicySnapshot, PrincipalFlowSnapshot, PrincipalFlowsSnapshot,
    RuntimeSnapshot, StatsSnapshot, StatusSnapshot, UdpUpstreamStats, UrlTestSelectionSnapshot,
};
pub use traits::{
    ApiAuth, ApiCodec, CommandService, EventSink, EventSource, EventStream, EventStreamReceive,
    QueryService,
};

pub const API_ID: &str = "zero.api.v1";
pub const EVENT_SCHEMA_ID: &str = "zero.event.v1";
/// Schema version of the machine-readable capabilities manifest.
pub const CAPABILITIES_CONTRACT_VERSION: u32 = 1;
/// Version of query, command, response-envelope, and event control semantics.
pub const CONTROL_API_VERSION: u32 = 1;
/// Version of the accepted and exported runtime configuration JSON schema.
pub const CONFIG_SCHEMA_VERSION: u32 = 1;
/// Version of the stable API error-code catalog.
pub const ERROR_CODE_CONTRACT_VERSION: u32 = 1;

pub type ApiResult<T> = Result<T, ApiError>;
pub type RawApiEvent = ApiEvent<serde_json::Value>;
