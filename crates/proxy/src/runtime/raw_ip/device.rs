//! Shared raw-IP peer device for active TCP and UDP stacks.

mod completion;
mod driver;
mod endpoint;
mod health;
mod maintenance;
mod returns;
mod session_tcp;
mod session_udp;
mod standard;
pub(crate) use standard::PacketFlowBorrow;
mod standard_packet;
pub(crate) use returns::ReturnSource;
pub(crate) use standard_packet::PacketSubmission;
#[cfg(test)]
mod tests;
mod translation;
mod wire;
mod workers;

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};
use zero_api::OutboundDeviceHealthSnapshot;
use zero_engine::EngineError;
#[cfg(test)]
use zero_platform_tokio::TokioDatagramSocket;
use zero_stack::{client_udp::ClientUdpStack, ClientTcpStack, FragmentReassembler, UserTcpStream};

#[cfg(test)]
use super::DirectRawIpWireCarrier;
use super::{RawIpTunnel, RawIpWireCarrier};
use driver::{run_device, Device};
pub(crate) use endpoint::{EndpointPacket, EndpointStackOwner, EndpointStackRecipe};
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
    task: Arc<workers::DeviceWorkers>,
    carrier: Option<Arc<dyn RawIpWireCarrier>>,
}

impl SharedRawIpDevice {
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn is_usable(&self) -> bool {
        !self.retired.load(Ordering::Acquire) && !self.is_closed()
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
        if let Some(carrier) = &self.carrier {
            drop(carrier.stop_background());
        }
        self.tcp.revoke();
        self.udp.shutdown();
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
        // Raw packet return subscriptions are another transferred-user lane;
        // they do not inherit the PacketTcpOperation's caller/view token.
        if let Some(users) = self.task.flow_users() {
            users.mark_unaccounted();
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
        if let Some(users) = self.task.flow_users() {
            users.mark_unaccounted();
        }
        self.udp.bind_observed(local_ip, observer)
    }

    pub(crate) async fn open_tcp_observed(
        &self,
        local_ip: IpAddr,
        destination: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<UserTcpStream, zero_stack::ClientTcpStackError> {
        if let Some(users) = self.task.flow_users() {
            users.mark_unaccounted();
        }
        self.open_tcp_for_flow(local_ip, destination, observer, None)
            .await
    }
    pub(crate) fn spawn_packet_worker(
        &self,
        worker: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send + 'static>,
        >,
    ) -> Result<(), EngineError> {
        self.task.spawn_driver(worker)
    }
    pub(crate) fn flow_users(&self) -> Option<Arc<crate::runtime::packet_flow::FlowUsers>> {
        self.task.flow_users()
    }
    pub(crate) async fn open_tcp_for_flow(
        &self,
        local_ip: IpAddr,
        destination: SocketAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        user: Option<&crate::runtime::packet_flow::FlowUser>,
    ) -> Result<UserTcpStream, zero_stack::ClientTcpStackError> {
        if !self.is_usable() {
            return Err(zero_stack::ClientTcpStackError::OutboundClosed);
        }
        match (self.task.flow_users(), user) {
            (Some(users), Some(user)) if !users.validates(user) => {
                return Err(zero_stack::ClientTcpStackError::OutboundClosed)
            }
            (Some(users), None) => users.mark_unaccounted(),
            (None, Some(_)) => return Err(zero_stack::ClientTcpStackError::OutboundClosed),
            _ => {}
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

#[derive(Debug)]
enum StackConstructionError {
    Udp(zero_stack::client_udp::ClientUdpStackError),
    Tcp(zero_stack::ClientTcpStackError),
}
impl std::fmt::Display for StackConstructionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Udp(error) => write!(f, "raw-IP UDP stack: {error:?}"),
            Self::Tcp(error) => write!(f, "raw-IP TCP stack: {error:?}"),
        }
    }
}
impl std::error::Error for StackConstructionError {}
fn stack_udp_error(error: zero_stack::client_udp::ClientUdpStackError) -> EngineError {
    EngineError::Io(std::io::Error::other(StackConstructionError::Udp(error)))
}
fn stack_tcp_error(error: zero_stack::ClientTcpStackError) -> EngineError {
    EngineError::Io(std::io::Error::other(StackConstructionError::Tcp(error)))
}
