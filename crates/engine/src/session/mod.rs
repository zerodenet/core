//! Session lifecycle, registry, observations, accounting, and hooks.

mod completed;
mod hook;
mod lifecycle;
mod observation;
mod registry;
mod traffic;

pub use completed::CompletedSessionRecord;
pub use hook::{BlockReason, FlowContext, FlowHook, FlowHookChain, FlowTraffic};
pub use lifecycle::SessionHandle;
pub use observation::{
    FlowEgressObservation, FlowFailureObservation, FlowNetworkInterfaceObservation,
    FlowNetworkObservation, FlowPathObservation, FlowRemoteEndpoint, FlowRouteLookupObservation,
    FlowRouteObservation, FlowSocketBindingObservation, MatchedRouteRule,
};
pub use registry::ActiveSession;

pub(crate) use completed::CompletedSessionHistory;
pub(crate) use registry::{PrincipalFlowObservation, SessionRegistry, SessionTrafficUpdate};
