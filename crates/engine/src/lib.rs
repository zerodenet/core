mod api;
mod error;
mod groups;
mod health;
mod observability;
mod plan;
mod principal;
mod runtime;
mod session;

pub use api::{register_build_features, EngineHandle, EventSubscriber};
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
    EnginePlan, FallbackGroupPlan, LoadBalanceGroupPlan, OutboundIdentity, OutboundTarget,
    ResolvedLeafOutbound, ResolvedOutbound, SelectorGroupPlan, TargetId, TargetKind, TargetNode,
    UrlTestGroupPlan, UrlTestSelection, UrlTestSelectionReason,
};
pub use principal::{
    inspect_principal_quota_state, PrincipalCancellationRegistration, PrincipalDeviceRegistration,
    PrincipalQuotaStateReport, PrincipalQuotaStateStatus,
};
pub use runtime::{Engine, EngineRuntimeSnapshot};
pub use runtime::{RouteDecision, RouteTrace};
pub use session::{
    ActiveSession, BlockReason, CompletedSessionRecord, FlowContext, FlowFailureObservation,
    FlowHook, FlowHookChain, FlowNetworkInterfaceObservation, FlowNetworkObservation,
    FlowPathObservation, FlowRemoteEndpoint, FlowRouteLookupObservation, FlowRouteObservation,
    FlowSocketBindingObservation, FlowTraffic, MatchedRouteRule, SessionHandle,
};
pub use zero_api::{
    AddressSnapshot, AuthSnapshot, CompletedFlowSnapshot, ConfigSnapshot, FlowSnapshot,
    ListenerSnapshot, ModeSnapshot, OutboundTargetSnapshot, PolicyMemberSnapshot, PolicySnapshot,
    RuntimeSnapshot, StatsSnapshot, StatusSnapshot,
};
// Re-export stats sub-types from zero-api.
pub use zero_api::{OutboundTrafficStats, UdpUpstreamStats};
pub use zero_api::{PolicyProbeCompletedPayload, PolicyProbeMember};
