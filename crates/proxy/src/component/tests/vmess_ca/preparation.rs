//! The same cold preparation/build pipeline instruments real native File access.
use super::fixtures::*;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{ComponentInstance, ResourceAccess},
    lifecycle::BuildError,
    packet::BoxPacketSession,
    scope::TaskScope,
    BoxFuture,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
type Assembly = (ComponentInstance, TaskScope, Arc<HostResources>);
pub(super) fn prepare(
    address: SocketAddr,
    tls: &OwnedClientTlsProfile,
    ws: Option<&OwnedWebSocketProfile>,
    grpc: Option<&OwnedGrpcProfile>,
    base: &Path,
    bound: usize,
    observations: Arc<Observations>,
) -> Result<Assembly, zero_core::Error> {
    let options = try_options(address, tls, ws, grpc, base, bound)?;
    let path = base.join(tls.ca_cert_path.as_deref().expect("CA fixture path"));
    Ok(build_observed(options, &path, bound, Some(observations)))
}
pub(super) struct ObservedAccess {
    pub(super) grants: Arc<HostResources>,
    pub(super) observations: Arc<Observations>,
}
impl ResourceAccess for ObservedAccess {
    fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        self.observations.file_reads.fetch_add(1, Ordering::Relaxed);
        self.grants.read_file(path, max_bytes)
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        self.grants.listen_tcp(address)
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        self.grants.listen_datagram(address)
    }
    fn open_device(&self, name: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        self.grants.open_device(name)
    }
}
