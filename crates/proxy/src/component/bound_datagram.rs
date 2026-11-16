//! The same finite native provider is owned by the neutral runtime.
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod epochs;
pub(crate) use zero_runtime::component::bound_datagram::HostBoundDatagramResources;
