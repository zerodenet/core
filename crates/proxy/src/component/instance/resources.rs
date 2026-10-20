use super::invalid;
use std::{path::PathBuf, sync::Arc};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::ResourceAccess, packet::BoxPacketSession, BoxFuture, ChannelError, Reason,
};

// This transitional slot grants no I/O. Production catalog activation admits
// only zero-resource Stream descriptors; roleless maintenance can use its actual
// registered task budget. Declared I/O requires a host granting that exact access.
pub(crate) struct NoResources;
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared file access")) })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared listener")) })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared datagram listener")) })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared device")) })
    }
}
