//! Neutral raw-IP-over-datagram listener. Protocols own authentication and
//! packet codecs; this module owns socket, stack, route, and task lifetime.

mod icmp;
mod outer;
mod route;
mod run;
mod statistics;
use run::run;
mod tcp;

use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};
use zero_engine::EngineError;

use super::PreparedInboundListenerOperation;
use crate::runtime::raw_ip::{EndpointPacket, RawIpWireCarrier, SharedRawIpDevice};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
pub(crate) use icmp::IcmpEchoRelay;

pub(crate) enum RawIpInboundAction {
    SendNetwork(zero_traits::PacketBuffer),
    ReceiveIp(zero_traits::PacketBuffer),
}

pub(crate) struct RawIpInboundDispatch {
    pub(crate) peer_index: Option<usize>,
    pub(crate) authenticated: bool,
    pub(crate) actions: Vec<RawIpInboundAction>,
    /// Opaque device fact; runtime does not reimplement protocol source validation.
    pub(crate) source_rejected_packets: u64,
}

pub(crate) trait RawIpInboundDevice: Send {
    /// Protocol-owned assigned addresses, distinct from AllowedIPs prefixes.
    fn is_local_address(&self, _address: IpAddr) -> bool {
        false
    }
    fn peer_identity(&self, _peer: usize) -> Option<String> {
        None
    }
    fn generation(&self) -> u64 {
        0
    }
    fn mtu(&self) -> u16;
    fn peer_count(&self) -> usize;
    fn peer_for_destination(&self, destination: IpAddr) -> Option<usize>;
    fn receive_datagram(
        &mut self,
        source: Option<SocketAddr>,
        packet: &[u8],
    ) -> Result<RawIpInboundDispatch, EngineError>;
    fn send_ip_packet(
        &mut self,
        peer: usize,
        packet: &[u8],
    ) -> Result<Vec<RawIpInboundAction>, EngineError>;
    fn tick_peer(&mut self, peer: usize) -> Result<Vec<RawIpInboundAction>, EngineError>;
    fn handshake_age(&self, _peer: usize) -> Option<Duration> {
        None
    }
}

pub(crate) struct RawIpInboundListenerOperation {
    pub(crate) device: Box<dyn RawIpInboundDevice>,
    pub(crate) endpoint: Option<RawIpInboundEndpoint>,
}

pub(crate) struct RawIpInboundEndpoint {
    pub(crate) packets: mpsc::Receiver<EndpointPacket>,
    pub(crate) peers: watch::Receiver<Arc<EndpointPeerState>>,
    pub(crate) lease: Arc<AtomicBool>,
}

pub(crate) struct EndpointPeerState {
    pub(crate) revision: u64,
    pub(crate) devices: Vec<Arc<SharedRawIpDevice>>,
    pub(crate) initial_endpoints: Vec<Option<SocketAddr>>,
    pub(crate) carriers: Vec<Option<Arc<dyn RawIpWireCarrier>>>,
}

impl Drop for RawIpInboundEndpoint {
    fn drop(&mut self) {
        self.lease.store(false, Ordering::Release);
    }
}

impl PreparedInboundListenerOperation for RawIpInboundListenerOperation {
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let BoundInbound::Datagram(socket) = bound else {
                return Err(EngineError::Io(std::io::Error::other(
                    "expected datagram listener",
                )));
            };
            run(*self, runtime, socket, shutdown).await
        })
    }
}
