mod api;
mod config_query;
mod error;
mod groups;
pub use groups::{GroupSelectionBinding, GroupSelectionChanged, SelectionChange};
mod health;
mod observability;
mod plan;
mod principal;
mod runtime;
pub(crate) mod runtime_query;
pub use runtime_query::{EngineRuntimeQuery, EngineRuntimeQueryGuard, MissingFlow};
mod session;

pub use api::{register_build_features, EngineHandle, EventSubscriber};
pub use config_query::{EngineConfigQuery, MissingPolicy};
pub use error::EngineError;
pub type EventsSinceResult = zero_api::EventReplay;
// Re-export snapshot types from zero-api so downstream code doesn't need
// to import from two different crates for the same logical types.
pub use groups::{UrlTestGroupState, UrlTestMemberState};
pub use health::{
    PassiveRelayHealthKey, PassiveRelayOutcome, PassiveRelaySelection, ProbeTrigger,
    ProbeTriggerAck, ProbeTriggerRegistry,
};
pub use observability::SessionOutcome;
pub use plan::{
    EnginePlan, FallbackGroupPlan, LoadBalanceGroupPlan, OutboundIdentity, OutboundStaticBinding,
    OutboundTarget, ResolvedLeafOutbound, ResolvedOutbound, SelectorGroupPlan, TargetId,
    TargetKind, TargetNode, UrlTestGroupPlan, UrlTestSelection, UrlTestSelectionReason,
};
pub use principal::{
    inspect_principal_quota_state, inspect_principal_quota_state_with_source,
    PrincipalCancellationRegistration, PrincipalDeviceRegistration, PrincipalQuotaStateReport,
    PrincipalQuotaStateStatus,
};
pub use runtime::{
    ConfigurationPublication, EndpointAdmission, EndpointChange, Engine, EngineRuntimeSnapshot,
    OutboundHealthObservation, SessionIdentity,
};
pub use runtime::{IpRouteCache, RouteDecision, RouteEvaluation, RouteTrace};
pub use runtime::{PacketRouteControl, PacketRouteLease};
pub use session::{
    ActiveSession, AdmittedSession, BlockReason, CompletedSessionRecord,
    FlowAddressFamilyFallbackObservation, FlowConnectionAttemptObservation, FlowContext,
    FlowEgressObservation, FlowFailureObservation, FlowHook, FlowHookChain,
    FlowNetworkInterfaceObservation, FlowNetworkObservation, FlowPathObservation,
    FlowRemoteEndpoint, FlowRouteLookupObservation, FlowRouteObservation,
    FlowSocketBindingObservation, FlowTraffic, MatchedRouteRule, SessionHandle,
};
pub use zero_api::{
    AddressSnapshot, AuthSnapshot, CompletedFlowSnapshot, ConfigSnapshot, FlowSnapshot,
    ListenerSnapshot, ModeSnapshot, OutboundTargetSnapshot, PolicyMemberSnapshot, PolicySnapshot,
    RuntimeSnapshot, StatsSnapshot, StatusSnapshot,
};
pub use zero_router::RouteMode;
// Re-export stats sub-types from zero-api.
pub use zero_api::{OutboundTrafficStats, UdpUpstreamStats};
pub use zero_api::{PolicyProbeCompletedPayload, PolicyProbeMember};

pub use observability::traffic::{
    HostInterfaceSample, InboundTrafficRegistration, PreparedEndpointTraffic, TrafficMeter,
    TrafficRouteLease,
};
