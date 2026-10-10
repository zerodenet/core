use crate::runtime::{
    packet_flow::{
        PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowEndpoint,
        PacketFlowSource,
    },
    raw_ip::{EndpointPacket, SharedRawIpDevice},
};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, watch};
use zero_engine::EngineError;
use zero_stack::{ClientTcpStackError, ClientUdpSocket, ClientUdpStackError, UserTcpStream};
use zero_traits::IoObserver;

pub(crate) const LOCAL: &str = "10.0.0.1";
pub(crate) const REMOTE: &str = "10.0.0.2";
pub(crate) fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}
pub(crate) fn address(ip: IpAddr) -> zero_core::Address {
    match ip {
        IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
        IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
    }
}
pub(crate) fn target() -> SocketAddr {
    "10.0.0.2:443".parse().unwrap()
}

/// Pure IP endpoint: no WireGuard, tunnel engine, encryption or network socket.
pub(crate) struct Fixture {
    pub(crate) device: Arc<SharedRawIpDevice>,
    pub(crate) packets: mpsc::Receiver<EndpointPacket>,
    pub(crate) adapter: PacketFlowAdapter,
    pub(crate) generation: Arc<AtomicU64>,
    _publication: watch::Sender<mpsc::Sender<EndpointPacket>>,
}
impl Fixture {
    pub(crate) fn new(mtu: u16) -> Self {
        let (sender, packets) = mpsc::channel(128);
        let (publication, receiver) = watch::channel(sender);
        let device =
            SharedRawIpDevice::start_on_endpoint(vec![ip(LOCAL)], mtu, 0, receiver).unwrap();
        let generation = Arc::new(AtomicU64::new(0));
        let adapter = PacketFlowAdapter::new(
            "packet-only".into(),
            Arc::new(Source {
                device: device.clone(),
                generation: generation.clone(),
            }),
        );
        Self {
            device,
            packets,
            adapter,
            generation,
            _publication: publication,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.device.close_now();
    }
}

struct Source {
    device: Arc<SharedRawIpDevice>,
    generation: Arc<AtomicU64>,
}
impl PacketFlowSource for Source {
    fn bind(
        &self,
        target: IpAddr,
        generation: u64,
    ) -> Result<PacketFlowBinding, PacketFlowBindError> {
        if target != ip(REMOTE) {
            return Err(PacketFlowBindError::UnsupportedTarget(EngineError::Io(
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "target outside this Packet endpoint",
                ),
            )));
        }
        Ok(PacketFlowBinding {
            local_ip: ip(LOCAL),
            peer_id: None,
            endpoint: Arc::new(Endpoint {
                device: self.device.clone(),
                generation: self.generation.clone(),
                expected: generation,
            }),
        })
    }
}
struct Endpoint {
    device: Arc<SharedRawIpDevice>,
    generation: Arc<AtomicU64>,
    expected: u64,
}
#[async_trait::async_trait]
impl PacketFlowEndpoint for Endpoint {
    fn is_current(&self) -> bool {
        self.device.is_usable() && self.generation.load(Ordering::Acquire) == self.expected
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
