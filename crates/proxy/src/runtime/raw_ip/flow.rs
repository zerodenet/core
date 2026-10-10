//! Bridge a validated raw-IP plan and generation-fenced shared device into
//! the common Flow -> Packet converter. No WireGuard types are needed.
use super::{RawIpDevicePool, RawIpOutboundPlan, SharedRawIpDevice};
use crate::runtime::packet_flow::{
    PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowEndpoint, PacketFlowSource,
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use zero_engine::EngineError;
use zero_stack::{ClientTcpStackError, ClientUdpSocket, ClientUdpStackError, UserTcpStream};
use zero_traits::IoObserver;

pub(crate) struct RawIpFlowSource {
    pub(crate) tag: Arc<str>,
    pub(crate) identity: [u8; 32],
    pub(crate) plan: Arc<dyn RawIpOutboundPlan>,
    pub(crate) pool: Arc<RawIpDevicePool>,
}

impl RawIpFlowSource {
    pub(crate) fn adapter(
        tag: String,
        identity: [u8; 32],
        plan: Arc<dyn RawIpOutboundPlan>,
        pool: Arc<RawIpDevicePool>,
    ) -> PacketFlowAdapter {
        let tag: Arc<str> = tag.into();
        PacketFlowAdapter::new(
            tag.clone(),
            Arc::new(Self {
                tag,
                identity,
                plan,
                pool,
            }),
        )
    }
}

impl PacketFlowSource for RawIpFlowSource {
    fn bind(
        &self,
        target: IpAddr,
        generation: u64,
    ) -> Result<PacketFlowBinding, PacketFlowBindError> {
        let peer = self
            .plan
            .peer_for_target(target)
            .map_err(PacketFlowBindError::UnsupportedTarget)?;
        let cell = self
            .pool
            .cell_for(&self.tag, peer.peer_index, self.identity, generation)
            .map_err(PacketFlowBindError::Failed)?;
        let device = cell
            .get()
            .ok_or_else(|| {
                EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "peer device not initialized",
                ))
            })
            .map_err(PacketFlowBindError::Failed)?
            .clone();
        Ok(PacketFlowBinding {
            local_ip: peer.local_ip,
            peer_id: self.plan.peer_identity(peer.peer_index),
            endpoint: Arc::new(RawIpFlowEndpoint {
                tag: self.tag.clone(),
                identity: self.identity,
                generation,
                peer: peer.peer_index,
                pool: self.pool.clone(),
                device,
            }),
        })
    }
}

struct RawIpFlowEndpoint {
    tag: Arc<str>,
    identity: [u8; 32],
    generation: u64,
    peer: usize,
    pool: Arc<RawIpDevicePool>,
    device: Arc<SharedRawIpDevice>,
}

#[async_trait::async_trait]
impl PacketFlowEndpoint for RawIpFlowEndpoint {
    fn is_current(&self) -> bool {
        self.device.is_usable()
            && self.pool.is_current(
                &self.tag,
                self.peer,
                self.identity,
                self.generation,
                &self.device,
            )
    }
    fn is_closed(&self) -> bool {
        self.device.is_closed()
    }
    fn bind_udp(
        &self,
        local: IpAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<ClientUdpSocket, ClientUdpStackError> {
        self.device.bind_udp_observed(local, observer)
    }
    async fn open_tcp(
        &self,
        local: IpAddr,
        target: SocketAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        self.device.open_tcp_observed(local, target, observer).await
    }
}

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
