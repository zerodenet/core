//! Neutral raw-IP-over-datagram listener. Protocols own authentication and
//! packet codecs; this module owns socket, stack, route, and task lifetime.

mod children;
mod icmp;
mod lifecycle;
pub(crate) mod native_obligations;
mod observation;
mod outer;
mod route;
mod run;
mod socket;
pub(crate) mod source;
mod stack_tasks;
mod statistics;
mod wire;
pub(in crate::runtime) use run::run;
pub(crate) mod tasks;
mod tcp;

use std::{
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::{mpsc, watch};
use zero_engine::EngineError;

use crate::runtime::raw_ip::{EndpointPacket, RawIpWireCarrier, SharedRawIpDevice};
use crate::runtime::route_runtime::InboundListenerRuntime;
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
    /// Only an authenticated protocol-owned source observation may seed roaming
    /// continuity after the original listener has actually closed.
    fn authenticated_endpoint(&self, _peer: usize) -> Option<SocketAddr> {
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
    fn timer_schedule(&self, _peer: usize) -> crate::runtime::raw_ip::timer::TimerSchedule {
        crate::runtime::raw_ip::timer::TimerSchedule::Polling
    }
    fn handshake_age(&self, _peer: usize) -> Option<Duration> {
        None
    }
}

pub(crate) struct PacketSourceContext {
    pub original: PacketSourceOriginal,
    children: children::RawIpChildren,
}
pub(crate) enum PacketSourceOriginal {
    Listener(zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity),
    Device(zero_platform_tokio::contract::component::PacketDeviceIdentity),
}
pub(crate) struct PreparedDevicePacketSource {
    instance: zero_core::component::ComponentInstanceId,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    children: children::RawIpChildren,
}
impl PreparedDevicePacketSource {
    /// Complete only from the concrete acquired host provider; cold preparation
    /// cannot mint a PacketDeviceIdentity, native controller or device grant.
    pub(crate) fn complete(
        self,
        actual: &zero_platform_tokio::controlled_carrier::PacketProviderRegistration,
    ) -> Result<PacketSourceContext, EngineError> {
        if actual.instance() != self.instance
            || !actual.device().scope().same_scope(&self.scope)
            || !actual.driver().scope_identity().same_scope(&self.scope)
        {
            return Err(std::io::Error::other(
                "device source differs from its actual cold host instance/Scope",
            )
            .into());
        }
        Ok(PacketSourceContext {
            original: PacketSourceOriginal::Device(actual.device().clone()),
            children: self.children,
        })
    }
}
impl PacketSourceContext {
    /// Install every child responsibility before the main driver snapshots the
    /// original owner. This acquires no device/native IO and starts no task.
    pub(crate) fn prepare_device_source(
        context: &crate::runtime::inbound_owners::InboundInstallContext,
    ) -> Result<PreparedDevicePacketSource, EngineError> {
        let mut children = children::RawIpChildren::new();
        children.install(context)?;
        Ok(PreparedDevicePacketSource {
            instance: context.owner().identity(),
            scope: context.tasks().peer_registrar().scope_identity(),
            children,
        })
    }

    pub(crate) const MAX_ICMP_PROBES: usize = icmp::MAX_ECHO_PROBES;
    pub(crate) fn internal_task_limit() -> usize {
        tcp::MAX_CONNECTIONS + crate::inbound::tun::udp::OwnedTunUdpTasks::MAX_ASSOCIATIONS + 1
    }
    pub(in crate::runtime) fn install(
        context: &crate::runtime::inbound_owners::InboundInstallContext,
        original: zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity,
    ) -> Result<Self, EngineError> {
        let mut children = children::RawIpChildren::new();
        children.install(context)?;
        Ok(Self {
            original: PacketSourceOriginal::Listener(original),
            children,
        })
    }
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

#[cfg(test)]
pub(crate) use lifecycle::retained_endpoint as retained_endpoint_for_test;
