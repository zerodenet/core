//! Neutral raw-IP-over-datagram listener. Protocols own authentication and
//! packet codecs; this module owns socket, stack, route, and task lifetime.

mod icmp;
mod outer;
mod route;
mod tcp;

use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use zero_engine::EngineError;
use zero_stack::{packet, FragmentOutcome, FragmentReassembler, UserNetworkStack};

use super::PreparedInboundListenerOperation;
use crate::runtime::packet_route::PacketSessionPins;
use crate::runtime::raw_ip::{EndpointPacket, RawIpWireCarrier, SharedRawIpDevice};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
pub(crate) use icmp::IcmpEchoRelay;
use outer::{peer_carrier, refresh_endpoint_peers, send_network_actions, ProxiedWirePacket};
use route::feed_inner_packet;

static NEXT_PACKET_INGRESS_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) enum RawIpInboundAction {
    SendNetwork(Vec<u8>),
    ReceiveIp(Vec<u8>),
}

pub(crate) struct RawIpInboundDispatch {
    pub(crate) peer_index: Option<usize>,
    pub(crate) authenticated: bool,
    pub(crate) actions: Vec<RawIpInboundAction>,
}

pub(crate) trait RawIpInboundDevice: Send {
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
    pub(crate) initial_endpoints: Vec<SocketAddr>,
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

async fn run(
    mut operation: RawIpInboundListenerOperation,
    runtime: InboundListenerRuntime,
    socket: zero_platform_tokio::PacketSocket,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError> {
    let mtu = operation.device.mtu();
    let (response_tx, mut responses) = mpsc::channel::<Vec<u8>>(256);
    let (tcp, udp) =
        UserNetworkStack::new(response_tx.clone(), zero_stack::tcp_mss_for_mtu(mtu)).into_parts();
    let mut tasks = JoinSet::new();
    tasks.spawn(tcp::accept(tcp.clone(), runtime.route_factory()));
    tasks.spawn(crate::inbound::tun::udp::run_with_runtime(
        runtime.udp_runtime(),
        udp.clone(),
        runtime.route_factory().inbound_tag().to_owned(),
        false,
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
    ));
    let initial_endpoints = operation
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.peers.borrow().initial_endpoints.clone())
        .unwrap_or_default();
    let mut peer_revision = u64::MAX;
    let mut initial_endpoints = initial_endpoints;
    let mut endpoints = (0..operation.device.peer_count())
        .map(|peer| initial_endpoints.get(peer).copied())
        .collect::<Vec<_>>();
    let mut generation = operation.device.generation();
    let (proxied_tx, mut proxied_rx) = mpsc::channel::<ProxiedWirePacket>(256);
    let mut proxied_tasks = JoinSet::new();
    let mut peer_uses_proxy = Vec::new();
    let mut buffer = vec![0; 65_536];
    let mut fragments = FragmentReassembler::new();
    let mut endpoint_fragments = FragmentReassembler::new();
    let echo = IcmpEchoRelay::new(
        response_tx.clone(),
        runtime.route_factory(),
        shutdown.clone(),
    );
    let ingress_id = NEXT_PACKET_INGRESS_ID.fetch_add(1, Ordering::Relaxed);
    let packet_route = runtime.route_factory();
    let mut packet_pins = PacketSessionPins::default();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tcp_cleanup = tokio::time::interval(Duration::from_secs(30));
    tcp_cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut fragment_id = 1_u32;
    refresh_endpoint_peers(
        operation.endpoint.as_mut(),
        &mut peer_revision,
        &mut initial_endpoints,
        &mut endpoints,
        &mut fragments,
        &mut endpoint_fragments,
        &mut proxied_tasks,
        &proxied_tx,
        &mut peer_uses_proxy,
    );
    let outcome = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break Ok(()); }
            }
            received = async {
                tokio::select! {
                    direct = socket.recv_from(&mut buffer) => direct.map(|(size, source)| (size, Some(source), None, peer_revision)),
                    proxied = proxied_rx.recv() => match proxied {
                        Some(packet) => {
                            let size = packet.bytes.len();
                            if size <= buffer.len() { buffer[..size].copy_from_slice(&packet.bytes); }
                            Ok((size, packet.source, Some(packet.peer), packet.revision))
                        }
                        None => std::future::pending().await,
                    },
                }
            } => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                let (size, source, proxied_peer, received_revision) = received.map_err(EngineError::Io)?;
                if size > 65_535 || received_revision != peer_revision { continue; }
                let dispatch = match operation.device.receive_datagram(source, &buffer[..size]) {
                    Ok(dispatch) => dispatch,
                    Err(error) => { tracing::debug!(%error, "raw-IP inbound rejected datagram"); continue; }
                };
                if proxied_peer.is_some_and(|expected| dispatch.peer_index.is_some_and(|actual| expected != actual)) {
                    tracing::debug!(expected_peer = ?proxied_peer, authenticated_peer = ?dispatch.peer_index, "raw-IP outer carrier peer mismatch");
                    continue;
                }
                tracing::trace!(wire_bytes = size, peer = ?dispatch.peer_index, authenticated = dispatch.authenticated, actions = dispatch.actions.len(), "raw-IP inbound datagram processed");
                if dispatch.authenticated {
                    if let Some(peer) = dispatch.peer_index {
                        if let Some(route) = peer_uses_proxy.get_mut(peer) { *route = proxied_peer == Some(peer); }
                        if let (Some(endpoint), Some(source)) = (endpoints.get_mut(peer), source) { *endpoint = Some(source); }
                        if let Some(device) = operation.endpoint.as_ref().and_then(|endpoint| endpoint.peers.borrow().devices.get(peer).cloned()) {
                            device.observe_handshake(operation.device.handshake_age(peer));
                        }
                    }
                }
                let carrier = proxied_peer.and_then(|peer| peer_carrier(&operation.endpoint, peer));
                let response_target = source.or_else(|| proxied_peer.and_then(|peer| endpoints.get(peer).and_then(|value| *value)));
                if let Some(response_target) = response_target {
                    send_network_actions(&socket, carrier, response_target, &dispatch.actions).await;
                }
                for action in dispatch.actions {
                    let RawIpInboundAction::ReceiveIp(packet) = action else { continue; };
                    tracing::trace!(ip_bytes = packet.len(), "raw-IP inbound inner packet");
                    if let Some(peer) = dispatch.peer_index {
                        if let Some(device) = operation.endpoint.as_ref().and_then(|endpoint| endpoint.peers.borrow().devices.get(peer).cloned()) {
                            let processed = endpoint_fragments.process(&packet, Instant::now());
                            let reassembled = matches!(processed, FragmentOutcome::Reassembled(_));
                            let packet = match processed {
                                FragmentOutcome::NotFragmented(packet) => packet.to_vec(),
                                FragmentOutcome::Reassembled(packet) => packet,
                                FragmentOutcome::Pending | FragmentOutcome::Rejected(_) => continue,
                            };
                            device.observe_authenticated_packet();
                            if device.deliver_decrypted(&packet).await { continue; }
                            let mtu = if reassembled {
                                operation.device.mtu().max(packet.len().min(u16::MAX as usize) as u16)
                            } else {
                                operation.device.mtu()
                            };
                            feed_inner_packet(&packet, mtu, &tcp, &udp, &response_tx, &echo, &packet_route, ingress_id, &mut packet_pins, &mut fragments).await;
                            continue;
                        }
                    }
                    feed_inner_packet(&packet, operation.device.mtu(), &tcp, &udp, &response_tx, &echo, &packet_route, ingress_id, &mut packet_pins, &mut fragments).await;
                }
            }
            outgoing = receive_endpoint_packet(&mut operation.endpoint) => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                let Some(outgoing) = outgoing else {
                    operation.endpoint = None;
                    continue;
                };
                let Some(address) = endpoints.get(outgoing.peer).and_then(|value| *value) else { continue; };
                let carrier = peer_uses_proxy.get(outgoing.peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, outgoing.peer)).flatten();
                let packets = packet::fragment_ip_packet(&outgoing.packet, operation.device.mtu() as usize, fragment_id);
                fragment_id = fragment_id.wrapping_add(1);
                for packet in packets {
                    let actions = operation.device.send_ip_packet(outgoing.peer, &packet)?;
                    send_network_actions(&socket, carrier.clone(), address, &actions).await;
                }
            }
            response = responses.recv() => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                let Some(response) = response else { break Err(EngineError::Io(std::io::Error::other("raw-IP response channel closed"))); };
                tracing::trace!(ip_bytes = response.len(), "raw-IP inbound stack response");
                let Some(destination) = packet::ip_destination(&response) else { continue; };
                let Some(peer) = operation.device.peer_for_destination(destination) else { continue; };
                let Some(endpoint) = endpoints.get(peer).and_then(|value| *value) else { continue; };
                let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                let packets = packet::fragment_ip_packet(&response, operation.device.mtu() as usize, fragment_id);
                tracing::trace!(fragments = packets.len(), "raw-IP inbound response fragments");
                fragment_id = fragment_id.wrapping_add(1);
                for fragment in packets {
                    match operation.device.send_ip_packet(peer, &fragment) {
                        Ok(actions) => send_network_actions(&socket, carrier.clone(), endpoint, &actions).await,
                        Err(error) => tracing::debug!(%error, "raw-IP inbound response encode failed"),
                    }
                }
            }
            _ = tick.tick() => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                for (peer, endpoint) in endpoints.iter().enumerate() {
                    let Some(endpoint) = endpoint else { continue; };
                    let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                    match operation.device.tick_peer(peer) {
                        Ok(actions) => send_network_actions(&socket, carrier, *endpoint, &actions).await,
                        Err(error) => tracing::debug!(%error, peer, "raw-IP inbound timer failed"),
                    }
                }
            }
            _ = tcp_cleanup.tick() => {
                tcp.cleanup_idle(Duration::from_secs(300)).await;
            }
            finished = tasks.join_next() => {
                break match finished {
                    Some(Ok(result)) => result,
                    Some(Err(error)) => Err(EngineError::Io(std::io::Error::other(error))),
                    None => Err(EngineError::Io(std::io::Error::other("raw-IP ingress tasks ended"))),
                };
            }
            _ = proxied_tasks.join_next(), if !proxied_tasks.is_empty() => {}
        }
    };
    tasks.shutdown().await;
    proxied_tasks.shutdown().await;
    outcome
}

async fn receive_endpoint_packet(
    endpoint: &mut Option<RawIpInboundEndpoint>,
) -> Option<EndpointPacket> {
    match endpoint {
        Some(endpoint) => endpoint.packets.recv().await,
        None => std::future::pending().await,
    }
}

fn refresh_device_generation(
    device: &dyn RawIpInboundDevice,
    generation: &mut u64,
    endpoints: &mut Vec<Option<SocketAddr>>,
    fragments: &mut FragmentReassembler,
    endpoint_fragments: &mut FragmentReassembler,
    initial_endpoints: &[SocketAddr],
) {
    let current = device.generation();
    if *generation != current {
        *generation = current;
        *endpoints = (0..device.peer_count())
            .map(|peer| initial_endpoints.get(peer).copied())
            .collect();
        *fragments = FragmentReassembler::new();
        *endpoint_fragments = FragmentReassembler::new();
    }
}
