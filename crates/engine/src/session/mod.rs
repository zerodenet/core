//! Session lifecycle, registry, observations, accounting, and hooks.

mod admitted;
mod completed;
mod hook;
mod lifecycle;
mod observation;
mod registry;
mod traffic;

pub(crate) use admitted::AdmissionLife;
pub use admitted::AdmittedSession;
pub use completed::CompletedSessionRecord;
pub use hook::{BlockReason, FlowContext, FlowHook, FlowHookChain, FlowTraffic};
pub use lifecycle::SessionHandle;
pub use observation::{
    FlowAddressFamilyFallbackObservation, FlowConnectionAttemptObservation, FlowEgressObservation,
    FlowFailureObservation, FlowNetworkInterfaceObservation, FlowNetworkObservation,
    FlowPathObservation, FlowRemoteEndpoint, FlowRouteLookupObservation, FlowRouteObservation,
    FlowSocketBindingObservation, MatchedRouteRule,
};
pub use registry::ActiveSession;

pub(crate) use completed::CompletedSessionHistory;
pub(crate) use registry::{PrincipalFlowObservation, SessionRegistry, SessionTrafficUpdate};

mod accounting;
