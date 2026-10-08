//! Shared raw-IP peer device for active TCP and UDP stacks.

mod completion;
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
#[cfg(test)]
use zero_platform_tokio::TokioDatagramSocket;
use zero_stack::{client_udp::ClientUdpStack, ClientTcpStack, FragmentReassembler, UserTcpStream};

#[cfg(test)]
use super::DirectRawIpWireCarrier;
use super::{RawIpTunnel, RawIpWireCarrier};
use driver::{run_device, Device};
pub(crate) use endpoint::EndpointPacket;
use health::DeviceHealth;
pub(crate) use returns::PacketReturns;

pub(super) struct ForwardedPackets {
    return_channel: zero_stack::packet_output::PacketSender,
    packets: Vec<zero_traits::PacketBuffer>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

pub(crate) struct SharedRawIpDevice {
    pub(crate) incarnation: u64,
    udp: ClientUdpStack,
    tcp: Arc<ClientTcpStack>,
    forwarded_packets: mpsc::Sender<ForwardedPackets>,
    returns: Arc<PacketReturns>,
    closed: Arc<AtomicBool>,
    health: Arc<Mutex<DeviceHealth>>,
    retired: AtomicBool,
    ready: watch::Receiver<Option<Result<(), String>>>,
    task: AbortHandle,
    completed: watch::Receiver<bool>,
}

impl SharedRawIpDevice {
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_usable(&self) -> bool {
        !self.retired.load(Ordering::Acquire) && !self.is_closed()
    }

    #[cfg(test)]
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

    #[cfg(test)]
    pub(crate) fn start_with_carrier(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
    ) -> Result<Arc<Self>, zero_stack::client_udp::ClientUdpStackError> {
        Self::start_metered(
            local_addresses,
            mtu,
            endpoint,
            carrier,
            tunnel,
            Default::default(),
        )
    }

    pub(crate) fn start_metered(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        endpoint: SocketAddr,
        carrier: Arc<dyn RawIpWireCarrier>,
        tunnel: Box<dyn RawIpTunnel>,
        traffic: super::RawIpTraffic,
    ) -> Result<Arc<Self>, zero_stack::client_udp::ClientUdpStackError> {
        let (outbound, raw_packets) =
            mpsc::channel::<zero_stack::packet_output::ObservedPacket>(128);
        let (forwarded_packets, forwarded_rx) = mpsc::channel(128);
        let udp = ClientUdpStack::new_observed_with_drops(
            local_addresses.clone(),
            outbound.clone(),
            mtu,
            Some(Arc::new(traffic.clone())),
        )?;
        let tcp = Arc::new(
            ClientTcpStack::new_observed(local_addresses, outbound.clone(), mtu)
                .map_err(|_| zero_stack::client_udp::ClientUdpStackError::InvalidMtu)?,
        );
        let returns = Arc::new(PacketReturns::with_drop_observer(Some(Arc::new(
            traffic.clone(),
        ))));
        let closed = Arc::new(AtomicBool::new(false));
        let health = Arc::new(Mutex::new(DeviceHealth::default()));
        let (ready_tx, ready) = watch::channel(None);
        let (completed_tx, completed) = watch::channel(false);
        let completion = completion::Completion(completed_tx);
        let driver = Device {
            traffic,
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
        };
        let task = tokio::spawn(async move {
            let _completion = completion;
            run_device(driver, ready_tx).await;
        })
        .abort_handle();
        Ok(Arc::new(Self {
            incarnation: super::next_incarnation(),
            udp,
            tcp,
            forwarded_packets,
            returns,
            closed,
            health,
            retired: AtomicBool::new(false),
            ready,
            task,
            completed,
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
        packets: &mut Vec<zero_traits::PacketBuffer>,
        source: IpAddr,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        conversation: Option<zero_stack::packet::PacketConversationKey>,
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
        let return_channel = replies.clone();
        self.returns.register_observed(
            source,
            ingress_id,
            replies,
            observer.clone(),
            conversation,
        )?;
        permit.send(ForwardedPackets {
            packets: std::mem::take(packets),
            observer,
            return_channel,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn bind_udp(
        &self,
        local_ip: IpAddr,
    ) -> Result<zero_stack::client_udp::ClientUdpSocket, zero_stack::client_udp::ClientUdpStackError>
    {
        if self.retired.load(Ordering::Acquire) || self.is_closed() {
            return Err(zero_stack::client_udp::ClientUdpStackError::OutboundClosed);
        }
        self.bind_udp_observed(local_ip, None)
    }

    pub(crate) fn bind_udp_observed(
        &self,
        local_ip: IpAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<zero_stack::ClientUdpSocket, zero_stack::ClientUdpStackError> {
        if !self.is_usable() {
            return Err(zero_stack::ClientUdpStackError::OutboundClosed);
        }
        self.udp.bind_observed(local_ip, observer)
    }

    pub(crate) async fn open_tcp_observed(
        &self,
        local_ip: IpAddr,
        destination: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<UserTcpStream, zero_stack::ClientTcpStackError> {
        if !self.is_usable() {
            return Err(zero_stack::ClientTcpStackError::OutboundClosed);
        }
        self.tcp
            .connect_observed(local_ip, destination, observer)
            .await
    }
}

impl Drop for SharedRawIpDevice {
    fn drop(&mut self) {
        self.close_now();
    }
}
