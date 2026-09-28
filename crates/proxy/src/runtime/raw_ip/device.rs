//! Shared raw-IP peer device for active TCP and UDP stacks.

mod driver;
mod endpoint;
mod health;
mod returns;
#[cfg(test)]
mod tests;
mod translation;

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, watch},
    task::AbortHandle,
};
use zero_api::OutboundDeviceHealthSnapshot;
use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;
use zero_stack::{client_udp::ClientUdpStack, ClientTcpStack, FragmentReassembler, UserTcpStream};

use super::{DirectRawIpWireCarrier, RawIpTunnel, RawIpWireCarrier};
use driver::{run_device, Device};
pub(crate) use endpoint::EndpointPacket;
use health::DeviceHealth;
use returns::PacketReturns;

pub(crate) struct SharedRawIpDevice {
    udp: ClientUdpStack,
    tcp: Arc<ClientTcpStack>,
    forwarded_packets: mpsc::Sender<Vec<Vec<u8>>>,
    returns: Arc<PacketReturns>,
    closed: Arc<AtomicBool>,
    health: Arc<Mutex<DeviceHealth>>,
    retired: AtomicBool,
    ready: watch::Receiver<Option<Result<(), String>>>,
    task: AbortHandle,
}

impl SharedRawIpDevice {
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_usable(&self) -> bool {
        !self.retired.load(Ordering::Acquire) && !self.is_closed()
    }

    pub(crate) fn start(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        socket: TokioDatagramSocket,
        tunnel: Box<dyn RawIpTunnel>,
    ) -> Result<Arc<Self>, zero_stack::client_udp::ClientUdpStackError> {
        Self::start_with_carrier(
            local_addresses,
            mtu,
            endpoint,
            Arc::new(DirectRawIpWireCarrier(socket)),
            tunnel,
        )
    }

    pub(crate) fn start_with_carrier(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
    ) -> Result<Arc<Self>, zero_stack::client_udp::ClientUdpStackError> {
        let (outbound, raw_packets) = mpsc::channel(128);
        let (forwarded_packets, forwarded_rx) = mpsc::channel(128);
        let udp = ClientUdpStack::new(local_addresses.clone(), outbound.clone(), mtu)?;
        let tcp = Arc::new(
            ClientTcpStack::new(local_addresses, outbound.clone(), mtu)
                .map_err(|_| zero_stack::client_udp::ClientUdpStackError::InvalidMtu)?,
        );
        let returns = Arc::new(PacketReturns::default());
        let closed = Arc::new(AtomicBool::new(false));
        let health = Arc::new(Mutex::new(DeviceHealth::default()));
        let (ready_tx, ready) = watch::channel(None);
        let task = tokio::spawn(run_device(
            Device {
                endpoint,
                carrier,
                tunnel,
                udp: udp.clone(),
                tcp: tcp.clone(),
                returns: returns.clone(),
                fragments: FragmentReassembler::new(),
                raw_packets,
                forwarded_packets: forwarded_rx,
                closed: closed.clone(),
                health: health.clone(),
            },
            ready_tx,
        ))
        .abort_handle();
        Ok(Arc::new(Self {
            udp,
            tcp,
            forwarded_packets,
            returns,
            closed,
            health,
            retired: AtomicBool::new(false),
            ready,
            task,
        }))
    }

    pub(crate) async fn wait_ready(&self) -> Result<(), EngineError> {
        let mut ready = self.ready.clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = ready.borrow_and_update().clone() {
                    return result.map_err(|error| EngineError::Io(std::io::Error::other(error)));
                }
                ready.changed().await.map_err(|_| {
                    EngineError::Io(std::io::Error::other("raw-IP device stopped before ready"))
                })?;
            }
        })
        .await
        .map_err(|_| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "raw-IP device readiness timed out",
            ))
        })?
    }

    pub(crate) fn retire_after(&self, grace: Duration) {
        if self.retired.swap(true, Ordering::AcqRel) {
            return;
        }
        let closed = self.closed.clone();
        let task = self.task.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                tokio::time::sleep(grace).await;
                closed.store(true, Ordering::Release);
                task.abort();
            });
        } else {
            self.close_now();
        }
    }

    pub(crate) fn close_now(&self) {
        self.retired.store(true, Ordering::Release);
        self.closed.store(true, Ordering::Release);
        self.task.abort();
        self.returns.clear();
    }

    pub(crate) fn health_snapshot(
        &self,
        tag: String,
        peer_index: usize,
    ) -> OutboundDeviceHealthSnapshot {
        self.health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .snapshot(tag, peer_index, self.is_closed())
    }

    pub(crate) fn forward_packets(
        &self,
        packets: Vec<Vec<u8>>,
        source: IpAddr,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
    ) -> std::io::Result<()> {
        if !self.is_usable() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "raw-IP device closed",
            ));
        }
        let permit = self
            .forwarded_packets
            .try_reserve()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        self.returns.register(source, ingress_id, replies)?;
        permit.send(packets);
        Ok(())
    }

    pub(crate) fn bind_udp(
        &self,
        local_ip: IpAddr,
    ) -> Result<zero_stack::client_udp::ClientUdpSocket, zero_stack::client_udp::ClientUdpStackError>
    {
        if self.retired.load(Ordering::Acquire) || self.is_closed() {
            return Err(zero_stack::client_udp::ClientUdpStackError::OutboundClosed);
        }
        self.udp.bind(local_ip)
    }

    pub(crate) async fn open_tcp(
        &self,
        local_ip: IpAddr,
        destination: SocketAddr,
    ) -> Result<UserTcpStream, zero_stack::ClientTcpStackError> {
        if self.retired.load(Ordering::Acquire) || self.is_closed() {
            return Err(zero_stack::ClientTcpStackError::OutboundClosed);
        }
        self.tcp.connect(local_ip, destination).await
    }
}

impl Drop for SharedRawIpDevice {
    fn drop(&mut self) {
        self.close_now();
    }
}
