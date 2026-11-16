//! Inbound route-runtime facade plus focused service/runtime builders.
//!
//! The root stays as a facade so shared ingress services, per-connection route
//! runtimes and listener runtimes do not regrow into
//! one large implementation bucket.

mod listener;
mod route;
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) use route::DatagramContinuityHost;
mod shared;
#[cfg(feature = "udp-runtime")]
mod stop;
#[cfg(feature = "udp-runtime")]
pub(crate) use stop::{ListenerStop, UdpStopGuard};

pub(crate) use listener::{InboundListenerRuntime, InboundListenerRuntimeFactory};
pub(crate) use route::{InboundRouteRuntime, InboundRouteRuntimeFactory};
pub(crate) use shared::{
    route_trace_for_session, route_trace_for_target, SharedIngressRuntimeServices,
};
