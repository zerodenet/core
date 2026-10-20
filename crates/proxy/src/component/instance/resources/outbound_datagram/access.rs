use super::*;
use std::path::PathBuf;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{packet::BoxPacketSession, BoxFuture};

impl ResourceAccess for HostOutboundDatagramResources {
    fn open_outbound_datagram(
        &self,
        request: OutboundDatagramRequest,
    ) -> Result<GrantedOutboundDatagram, ChannelError> {
        self.acquire(request)
    }
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::InvalidConfig,
                "undeclared file access",
            ))
        })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::InvalidConfig,
                "undeclared listener access",
            ))
        })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, ChannelError>,
    > {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::InvalidConfig,
                "undeclared listener access",
            ))
        })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async {
            Err(error(
                self.instance,
                Reason::InvalidConfig,
                "undeclared device access",
            ))
        })
    }
}
