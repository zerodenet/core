//! Inbound route-runtime facade plus focused service/runtime builders.
//!
//! The root stays as a facade so shared ingress services, per-connection route
//! runtimes, listener runtimes, and mux-substream helpers do not regrow into
//! one large implementation bucket.

mod listener;
#[cfg(feature = "managed-stream-runtime")]
mod mux;
mod route;
mod shared;
#[cfg(feature = "udp-runtime")]
mod stop;
#[cfg(feature = "udp-runtime")]
pub(crate) use stop::{ListenerStop, UdpStopGuard};

pub(crate) use listener::{InboundListenerRuntime, InboundListenerRuntimeFactory};
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use mux::MuxSubstreamRuntime;
pub(crate) use route::{InboundRouteRuntime, InboundRouteRuntimeFactory};
pub(crate) use shared::{route_trace_for_session, SharedIngressRuntimeServices};
