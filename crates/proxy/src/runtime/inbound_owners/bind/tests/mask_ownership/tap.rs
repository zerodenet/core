use super::*;
pub(super) struct Tap {
    pub(super) native: Arc<crate::component::instance::HostResources>,
    pub(super) address: Arc<std::sync::OnceLock<std::net::SocketAddr>>,
}
impl ResourceAccess for Tap {
    fn carrier_buffer_budget(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierBufferBudget> {
        self.native.carrier_buffer_budget()
    }
    fn packet_driver_metadata(&self) -> Result<PacketDriverMetadataView, BuildError> {
        self.native.packet_driver_metadata()
    }

    fn listen_metadata_datagram(
        &self,
        request: MetadataDatagramRequest,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<GrantedMetadataListener, BuildError>>
    {
        Box::pin(async move {
            let grant = self.native.listen_metadata_datagram(request).await?;
            self.address.set(grant.tx.local_addr().unwrap()).unwrap();
            Ok(grant)
        })
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt:&zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), BuildError> {
        self.native.confirm_metadata_users(ordinal, receipt, tasks)
    }
    fn read_file(
        &self,
        path: std::path::PathBuf,
        max: usize,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        self.native.read_file(path, max)
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::ControlledTcpListener, BuildError>,
    > {
        self.native.listen_tcp(address)
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<GrantedDatagramListener, BuildError>>
    {
        self.native.listen_datagram(address)
    }
    fn open_device(
        &self,
        key: Arc<str>,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::packet::BoxPacketSession, BuildError>,
    > {
        self.native.open_device(key)
    }
}
