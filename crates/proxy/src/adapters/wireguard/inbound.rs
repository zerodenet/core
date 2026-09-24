//! Thin WireGuard-to-neutral-raw-IP inbound bridge.

use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use wireguard::{
    runtime::{InboundDevice, PreparedInbound, TunnelAction},
    validation::{InboundInput, InboundPeerInput},
};
use zero_config::{InboundConfig, InboundProtocolConfig, WireguardSecret};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{
    PreparedInboundListenerOperation, RawIpInboundAction, RawIpInboundDevice, RawIpInboundDispatch,
    RawIpInboundListenerOperation,
};

pub(super) struct LiveInboundDevice {
    device: Mutex<InboundDevice>,
    generation: AtomicU64,
}

impl LiveInboundDevice {
    pub(super) fn new(device: InboundDevice) -> Self {
        Self {
            device: Mutex::new(device),
            generation: AtomicU64::new(0),
        }
    }

    pub(super) fn replace(&self, device: InboundDevice) {
        let mut current = self
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !current.replace_preserving_peers(device) {
            self.generation.fetch_add(1, Ordering::Release);
        }
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
    let live = Arc::new(LiveInboundDevice::new(device));
    Ok((
        Box::new(RawIpInboundListenerOperation {
            device: Box::new(WireguardInboundDevice(live.clone())),
        }),
        live,
    ))
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
        source: IpAddr,
        packet: &[u8],
    ) -> Result<RawIpInboundDispatch, EngineError> {
        let dispatch = self
            .0
            .device
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .receive_datagram(source, packet)
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
