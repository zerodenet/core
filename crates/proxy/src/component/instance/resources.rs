#[cfg(test)]
use super::invalid;
#[cfg(test)]
use std::{path::PathBuf, sync::Arc};
#[cfg(test)]
use zero_core::request::Destination;
#[cfg(test)]
use zero_platform_tokio::contract::{
    component::ResourceAccess, packet::BoxPacketSession, BoxFuture, ChannelError, Reason,
};
mod icmp;
mod native;
pub(crate) use icmp::{HostIcmpProbeGrant, HostIcmpProbeOwner};
pub(crate) use native::HostDatagramListenerOwner;
pub(in crate::component) use native::HostIoEvidence;
pub(in crate::component) use native::HostResources;
mod outbound_datagram;
pub(crate) use outbound_datagram::HostOutboundDatagramResources;

#[cfg(test)]
#[path = "../tests/resource_grants.rs"]
mod grant_tests;

// Test-only denial implementation. Real declared native acquisition is handled
// by HostResources; declaring a resource alone does not activate it in the catalog.
#[cfg(test)]
pub(crate) struct NoResources;
#[cfg(test)]
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
