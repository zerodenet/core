//! Active protocol ingress preparation and runtime-owned lifecycle.
#[cfg(feature = "managed-stream-runtime")]
mod lifecycle;
#[cfg(feature = "managed-stream-runtime")]
mod native;
#[cfg(feature = "managed-stream-runtime")]
mod operation;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use lifecycle::InboundServices;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use native::NativeServiceOperation;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) use operation::ServiceContext;
#[cfg(not(feature = "managed-stream-runtime"))]
mod disabled;
#[cfg(not(feature = "managed-stream-runtime"))]
pub(crate) use disabled::InboundServices;
