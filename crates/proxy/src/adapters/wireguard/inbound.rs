//! Thin WireGuard-to-neutral-raw-IP inbound bridge.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, watch};

use wireguard::{
    runtime::{InboundDevice, PreparedInbound, TunnelAction},
    validation::{InboundInput, InboundPeerInput},
};
use zero_config::{InboundConfig, InboundProtocolConfig, WireguardSecret};
use zero_engine::EngineError;

use super::inbound_protocol_identity;
use crate::runtime::inbound_operation::{
    EndpointPeerState, PreparedInboundListenerOperation, RawIpInboundAction, RawIpInboundDevice,
    RawIpInboundDispatch, RawIpInboundEndpoint, RawIpInboundListenerOperation,
};
use crate::runtime::raw_ip::{EndpointPacket, RawIpWireCarrier, SharedRawIpDevice};

pub(super) struct LinkedEndpoint {
    pub(super) listen: zero_config::ListenConfig,
    pub(super) live: Arc<LiveInboundDevice>,
    pub(super) state: Mutex<LinkedEndpointState>,
    pub(super) sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
    peers: watch::Sender<Arc<EndpointPeerState>>,
    packets: Mutex<Option<mpsc::Receiver<EndpointPacket>>>,
    active: Arc<AtomicBool>,
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
    pub(super) endpoints: Vec<SocketAddr>,
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
        let revision = self.link.peers.borrow().revision.wrapping_add(1);
        self.link.peers.send_replace(Arc::new(EndpointPeerState {
            revision,
            devices: self.devices.clone(),
            initial_endpoints: self.endpoints,
            carriers: self.carriers.clone(),
        }));
        state.identity = self.identity;
        state.generation = self.generation;
        state.devices = self.devices;
    }
}

impl LinkedEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        identity: [u8; 32],
        generation: u64,
        listen: zero_config::ListenConfig,
        live: Arc<LiveInboundDevice>,
        devices: Vec<Arc<SharedRawIpDevice>>,
        endpoints: Vec<SocketAddr>,
        carriers: Vec<Option<Arc<dyn RawIpWireCarrier>>>,
        sender: watch::Sender<mpsc::Sender<EndpointPacket>>,
        packets: mpsc::Receiver<EndpointPacket>,
    ) -> Self {
        let (peers, _) = watch::channel(Arc::new(EndpointPeerState {
            revision: 0,
            devices: devices.clone(),
            initial_endpoints: endpoints,
            carriers: carriers.clone(),
        }));
        Self {
            listen,
            live,
            state: Mutex::new(LinkedEndpointState {
                identity,
                generation,
                devices,
            }),
            sender,
            peers,
            packets: Mutex::new(Some(packets)),
            active: Arc::new(AtomicBool::new(false)),
        }
    }

    fn take_operation(&self) -> Result<Box<dyn PreparedInboundListenerOperation>, EngineError> {
        if self.active.swap(true, Ordering::AcqRel) {
            return Err(invalid(
                "linked WireGuard endpoint listener already running",
            ));
        }
        let packets = self
            .packets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .unwrap_or_else(|| {
                let (sender, packets) = mpsc::channel(256);
                self.sender.send_replace(sender);
                packets
            });
        Ok(Box::new(RawIpInboundListenerOperation {
            device: Box::new(WireguardInboundDevice(self.live.clone())),
            endpoint: Some(RawIpInboundEndpoint {
                packets,
                peers: self.peers.subscribe(),
                lease: self.active.clone(),
            }),
        }))
    }
}

pub(super) struct LiveInboundDevice {
    device: Mutex<InboundDevice>,
    generation: AtomicU64,
    identity: Mutex<[u8; 32]>,
}

impl LiveInboundDevice {
    pub(super) fn peer_source(
        &self,
        peer_index: usize,
    ) -> Option<wireguard::runtime::PeerSourceObservation> {
        self.device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_source(peer_index)
    }

    pub(super) fn new(device: InboundDevice, identity: [u8; 32]) -> Self {
        Self {
            device: Mutex::new(device),
            generation: AtomicU64::new(0),
            identity: Mutex::new(identity),
        }
    }

    pub(super) fn matches_identity(&self, identity: [u8; 32]) -> bool {
        *self
            .identity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            == identity
    }

    pub(super) fn replace(&self, device: InboundDevice, identity: [u8; 32]) {
        let mut current = self
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !current.replace_preserving_peers(device) {
            self.generation.fetch_add(1, Ordering::Release);
        }
        *self
            .identity
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = identity;
    }
}

pub(super) fn prepare(
    inbound: InboundConfig,
) -> Result<
    (
        Box<dyn PreparedInboundListenerOperation>,
        Arc<LiveInboundDevice>,
    ),
    EngineError,
> {
    let device = prepare_device(&inbound)?;
    let identity = inbound_protocol_identity(&inbound.protocol)
        .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
    let live = Arc::new(LiveInboundDevice::new(device, identity));
    Ok((
        Box::new(RawIpInboundListenerOperation {
            device: Box::new(WireguardInboundDevice(live.clone())),
            endpoint: None,
        }),
        live,
    ))
}

pub(super) fn prepare_unlinked_with_live(
    live: Arc<LiveInboundDevice>,
) -> Box<dyn PreparedInboundListenerOperation> {
    Box::new(RawIpInboundListenerOperation {
        device: Box::new(WireguardInboundDevice(live)),
        endpoint: None,
    })
}

pub(super) fn prepare_linked(
    linked: &Arc<LinkedEndpoint>,
) -> Result<
    (
        Box<dyn PreparedInboundListenerOperation>,
        Arc<LiveInboundDevice>,
    ),
    EngineError,
> {
    Ok((linked.take_operation()?, linked.live.clone()))
}

pub(super) fn prepare_device(inbound: &InboundConfig) -> Result<InboundDevice, EngineError> {
    let InboundProtocolConfig::Wireguard {
        private_key,
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
        mtu: *mtu,
        peers: &peers,
    })
    .map_err(invalid)?;
    profile.into_device().map_err(invalid)
}

struct WireguardInboundDevice(Arc<LiveInboundDevice>);

impl RawIpInboundDevice for WireguardInboundDevice {
    fn generation(&self) -> u64 {
        self.0.generation.load(Ordering::Acquire)
    }

    fn mtu(&self) -> u16 {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .mtu()
    }

    fn peer_count(&self) -> usize {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_count()
    }

    fn peer_for_destination(&self, destination: IpAddr) -> Option<usize> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .peer_for_destination(destination)
    }

    fn receive_datagram(
        &mut self,
        source: Option<SocketAddr>,
        packet: &[u8],
    ) -> Result<RawIpInboundDispatch, EngineError> {
        let dispatch = self
            .0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .receive_datagram_with_source(source, packet)
            .map_err(invalid)?;
        Ok(RawIpInboundDispatch {
            peer_index: dispatch.peer_index,
            authenticated: dispatch.authenticated,
            actions: dispatch.actions.into_iter().map(map_action).collect(),
        })
    }

    fn send_ip_packet(
        &mut self,
        peer: usize,
        packet: &[u8],
    ) -> Result<Vec<RawIpInboundAction>, EngineError> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .send_ip_packet(peer, packet)
            .map(|actions| actions.into_iter().map(map_action).collect())
            .map_err(invalid)
    }

    fn tick_peer(&mut self, peer: usize) -> Result<Vec<RawIpInboundAction>, EngineError> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .tick_peer(peer)
            .map(|actions| actions.into_iter().map(map_action).collect())
            .map_err(invalid)
    }

    fn handshake_age(&self, peer: usize) -> Option<std::time::Duration> {
        self.0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .time_since_last_handshake(peer)
    }
}

fn map_action(action: TunnelAction) -> RawIpInboundAction {
    match action {
        TunnelAction::SendNetwork(packet) => RawIpInboundAction::SendNetwork(packet),
        TunnelAction::ReceiveIp { packet, .. } => RawIpInboundAction::ReceiveIp(packet),
    }
}

fn invalid(error: impl std::fmt::Debug) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{error:?}"),
    ))
}
