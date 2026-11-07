//! Thin WireGuard-to-neutral-raw-IP inbound bridge.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio::sync::{mpsc, watch};

use wireguard::{
    runtime::{InboundDevice, PreparedInbound, TunnelAction},
    validation::{InboundInput, InboundPeerInput},
};
use zero_config::{InboundConfig, InboundProtocolConfig, WireguardSecret};
use zero_engine::EngineError;

mod codec;
pub(crate) mod cold;
mod device;
mod recovery;
use super::inbound_protocol_identity;
use crate::runtime::inbound_operation::{
    EndpointPeerState, RawIpInboundAction, RawIpInboundDevice, RawIpInboundDispatch,
    RawIpInboundEndpoint,
};
use crate::runtime::raw_ip::{EndpointPacket, RawIpWireCarrier, SharedRawIpDevice};
pub(super) use device::LiveInboundDevice;
mod io;
pub(in crate::adapters::wireguard) use io::EndpointIoRecipe;

pub(super) struct LinkedEndpoint {
    pub(super) listen: zero_config::ListenConfig,
    pub(super) live: Arc<LiveInboundDevice>,
    pub(super) io_incarnation: AtomicU64,
    pub(super) state: Mutex<LinkedEndpointState>,
    pub(super) sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
    io: Arc<io::EndpointIo>,
    pending_peers: Mutex<Option<Arc<EndpointPeerState>>>,
    owned_resources: OnceLock<Arc<super::lifecycle::PeerResources>>,
}

pub(super) struct LinkedEndpointState {
    pub(super) identity: [u8; 32],
    pub(super) generation: u64,
    pub(super) devices: Vec<Arc<SharedRawIpDevice>>,
}

pub(super) struct LinkedEndpointUpdate {
    pub(super) link: Arc<LinkedEndpoint>,
    pub(super) identity: [u8; 32],
    pub(super) generation: u64,
    pub(super) device: InboundDevice,
    pub(super) inbound_identity: [u8; 32],
    pub(super) devices: Vec<Arc<SharedRawIpDevice>>,
    pub(super) endpoints: Vec<Option<SocketAddr>>,
    pub(super) carriers: Vec<Option<Arc<dyn RawIpWireCarrier>>>,
}

impl LinkedEndpointUpdate {
    pub(super) fn publish(self) {
        let mut state = self
            .link
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.link.live.replace(self.device, self.inbound_identity);
        let revision = self.link.io.peers.borrow().revision.wrapping_add(1);
        self.link.io.peers.send_replace(Arc::new(EndpointPeerState {
            outer_memory: Vec::new(),
            prepared_outer: Vec::new(),
            revision,
            devices: self.devices.clone(),
            initial_endpoints: self.endpoints,
            carriers: self.carriers.clone(),
        }));
        if state.identity != self.identity || state.generation != self.generation {
            self.link.io_incarnation.store(
                crate::runtime::raw_ip::next_incarnation(),
                Ordering::Release,
            );
        }
        state.identity = self.identity;
        state.generation = self.generation;
        state.devices = self.devices;
    }
}

#[cfg(test)]
pub(super) fn prepare_linked(
    linked: &Arc<LinkedEndpoint>,
) -> Result<(RawIpInboundEndpoint, Arc<LiveInboundDevice>), EngineError> {
    Ok((linked.take_endpoint()?, linked.live.clone()))
}

pub(super) fn prepare_device(inbound: &InboundConfig) -> Result<InboundDevice, EngineError> {
    let InboundProtocolConfig::Wireguard {
        private_key,
        addresses,
        mtu,
        peers,
    } = &inbound.protocol
    else {
        return Err(invalid("expected WireGuard inbound"));
    };
    let allowed_ips = peers
        .iter()
        .map(|peer| {
            peer.allowed_ips
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let peers = peers
        .iter()
        .zip(&allowed_ips)
        .map(|(peer, allowed_ips)| InboundPeerInput {
            public_key: &peer.public_key,
            pre_shared_key: peer.pre_shared_key.as_ref().map(WireguardSecret::as_str),
            allowed_ips,
            keepalive_secs: peer.keepalive_secs,
            reserved: &peer.reserved,
        })
        .collect::<Vec<_>>();
    let profile = PreparedInbound::from_input(InboundInput {
        private_key: private_key.as_str(),
        addresses: &addresses.iter().map(String::as_str).collect::<Vec<_>>(),
        mtu: *mtu,
        peers: &peers,
    })
    .map_err(invalid)?;
    profile.into_device().map_err(invalid)
}

struct WireguardInboundDevice(Arc<LiveInboundDevice>);

fn invalid(error: impl std::fmt::Debug) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{error:?}"),
    ))
}

#[cfg(test)]
mod tests;
