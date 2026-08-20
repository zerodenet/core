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
    AdapterCapability, ApiCapabilities, CapabilityState, ProtocolCapability,
    ProtocolNetworkCapability, SinkCapability,
};
pub use command::{
    CommandRequest, CommandResponse, ConfigApplyCommand, ConfigValidateCommand,
    DiagnosticsDnsCacheCommand, DiagnosticsDnsLookupCommand, DiagnosticsFakeipLookupCommand,
    DiagnosticsProbeOutboundCommand, DiagnosticsProbeTargetCommand, DiagnosticsTraceRouteCommand,
    FlowCloseCommand, ModeSetCommand, PolicyProbeCommand, PolicySelectCommand, TunStartCommand,
    TunStopCommand,
};
pub use error::{ApiError, ApiErrorCode, ErrorDetail};
pub use event::{
    event_type, ApiEvent, EventFilter, EventReplay, PassiveRelayHealthChangedPayload,
    PassiveRelayHealthState, PublishResult,
};
pub use flow::{
    AuthInfo, EndpointRef, FlowEventPayload, FlowFailureInfo, FlowOutcome, FlowPath, FlowRecord,
    FlowRecordTiming, FlowResult, FlowRoute, FlowSource, FlowState, FlowTarget, FlowThroughput,
    FlowTiming, MatchedRuleInfo, Network, PolicyDecision, PolicyProbeCompletedPayload,
    PolicyProbeMember, PolicySelectedPayload, RouteDecision, TargetAddress, TrafficStats,
    WarningPayload,
};
pub use query::{
    CapabilitiesQuery, ConfigQuery, DiagnosticsQuery, FlowFilter, FlowGetQuery, FlowListQuery,
    HealthQuery, HealthSnapshot, PoliciesQuery, PolicyGetQuery, PrincipalFlowsQuery, QueryRequest,
    QueryResponse, RuntimeQuery, SinkStatusSnapshot, SinksQuery, StatsQuery, TunStatusQuery,
    TunStatusSnapshot,
};
pub use response::{ApiResponse, EnvelopeError, RawResponse};
pub use sink::{
    CallbackEventSink, DeadLetterSink, JsonLineEventSink, MemorySink, OutboxCorruptionClass,
    OutboxRecoveryState, OutboxRecoveryStatus, OutboxStorageStatus, RotatingFileSink, SinkManager,
    SinkStatus,
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

pub type ApiResult<T> = Result<T, ApiError>;
pub type RawApiEvent = ApiEvent<serde_json::Value>;
