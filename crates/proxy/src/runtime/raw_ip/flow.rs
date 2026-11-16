//! Explicit unit-fixture bridge to a directly owned stack. Production Flow
//! conversion runs through the installed device Stream/Datagram roles.
use super::SharedRawIpDevice;
use crate::runtime::packet_flow::PacketFlowEndpoint;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use zero_engine::EngineError;
use zero_stack::{ClientTcpStackError, ClientUdpSocket, ClientUdpStackError, UserTcpStream};
use zero_traits::IoObserver;

/// A directly owned, already prepared neutral IP endpoint needs no protocol
/// plan or device cache. Its owner handles admission/publication externally.
#[async_trait::async_trait]
impl PacketFlowEndpoint for SharedRawIpDevice {
    fn is_current(&self) -> bool {
        self.is_usable()
    }
    fn is_closed(&self) -> bool {
        SharedRawIpDevice::is_closed(self)
    }
    fn flow_users(&self) -> Option<Arc<crate::runtime::packet_flow::FlowUsers>> {
        self.flow_users()
    }
    async fn open_tcp_tracked(
        &self,
        local: IpAddr,
        target: SocketAddr,
        observer: Option<Arc<dyn IoObserver>>,
        user: Option<&crate::runtime::packet_flow::FlowUser>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        self.open_tcp_for_flow(local, target, observer, user).await
    }
    fn spawn_packet_worker(
        &self,
        worker: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send + 'static>,
        >,
    ) -> Result<(), EngineError> {
        self.spawn_packet_worker(worker)
    }
    fn bind_udp(
        &self,
        local: IpAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<ClientUdpSocket, ClientUdpStackError> {
        self.bind_udp_observed(local, observer)
    }
    async fn open_tcp(
        &self,
        local: IpAddr,
        target: SocketAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        self.open_tcp_observed(local, target, observer).await
    }
}
