#[cfg(any(test, feature = "test-support"))]
use super::invalid;
#[cfg(any(test, feature = "test-support"))]
use std::{path::PathBuf, sync::Arc};
#[cfg(any(test, feature = "test-support"))]
use zero_core::request::Destination;
#[cfg(any(test, feature = "test-support"))]
use zero_platform_tokio::contract::{
    BoxFuture, ChannelError, Reason, component::ResourceAccess, packet::BoxPacketSession,
};
mod icmp;
mod native;
pub use icmp::{HostIcmpProbeGrant, HostIcmpProbeOwner};
pub use native::HostDatagramListenerOwner;
pub use native::HostResources;
pub use native::{HostPacketReadReservation, packet_datagram_read_spec};
mod outbound_datagram;
pub use outbound_datagram::HostOutboundDatagramResources;

#[cfg(test)]
#[path = "../tests/resource_grants.rs"]
mod grant_tests;

// Test-only denial implementation. Real declared native acquisition is handled
// by HostResources; declaring a resource alone does not activate it in the catalog.
#[cfg(any(test, feature = "test-support"))]
pub struct NoResources;
#[cfg(any(test, feature = "test-support"))]
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared file access")) })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared listener")) })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, ChannelError>,
    > {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared datagram listener")) })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared device")) })
    }
}
