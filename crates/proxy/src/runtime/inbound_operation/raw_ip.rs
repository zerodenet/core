//! Neutral raw-IP-over-datagram listener. Protocols own authentication and
//! packet codecs; this module owns socket, stack, route, and task lifetime.

mod icmp;
mod tcp;

use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use zero_engine::EngineError;
use zero_stack::{
    packet, FragmentOutcome, FragmentReassembler, UserNetworkStack, UserTcpStack, UserUdpStack,
};
use zero_traits::{TcpStack, UdpStack};

use super::PreparedInboundListenerOperation;
use crate::inventory::PacketRouteTarget;
use crate::runtime::packet_route::{PacketPlane, PacketSessionPins};
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
pub(crate) use icmp::IcmpEchoRelay;

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
        source: IpAddr,
        packet: &[u8],
    ) -> Result<RawIpInboundDispatch, EngineError>;
    fn send_ip_packet(
        &mut self,
        peer: usize,
        packet: &[u8],
    ) -> Result<Vec<RawIpInboundAction>, EngineError>;
    fn tick_peer(&mut self, peer: usize) -> Result<Vec<RawIpInboundAction>, EngineError>;
}

pub(crate) struct RawIpInboundListenerOperation {
    pub(crate) device: Box<dyn RawIpInboundDevice>,
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
    let mut endpoints = vec![None; operation.device.peer_count()];
    let mut generation = operation.device.generation();
    let mut buffer = vec![0; 65_536];
    let mut fragments = FragmentReassembler::new();
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
    let outcome = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break Ok(()); }
            }
            received = socket.recv_from(&mut buffer) => {
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments);
                let (size, source) = received.map_err(EngineError::Io)?;
                if size > 65_535 { continue; }
                let dispatch = match operation.device.receive_datagram(source.ip(), &buffer[..size]) {
                    Ok(dispatch) => dispatch,
                    Err(error) => { tracing::debug!(%error, "raw-IP inbound rejected datagram"); continue; }
                };
                tracing::trace!(wire_bytes = size, peer = ?dispatch.peer_index, authenticated = dispatch.authenticated, actions = dispatch.actions.len(), "raw-IP inbound datagram processed");
                if dispatch.authenticated {
                    if let Some(peer) = dispatch.peer_index {
                        if let Some(endpoint) = endpoints.get_mut(peer) { *endpoint = Some(source); }
                    }
                }
                send_network_actions(&socket, source, &dispatch.actions).await?;
                for action in dispatch.actions {
                    let RawIpInboundAction::ReceiveIp(packet) = action else { continue; };
                    tracing::trace!(ip_bytes = packet.len(), "raw-IP inbound inner packet");
                    feed_inner_packet(&packet, operation.device.mtu(), &tcp, &udp, &response_tx, &echo, &packet_route, ingress_id, &mut packet_pins, &mut fragments).await;
                }
            }
            response = responses.recv() => {
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments);
                let Some(response) = response else { break Err(EngineError::Io(std::io::Error::other("raw-IP response channel closed"))); };
                tracing::trace!(ip_bytes = response.len(), "raw-IP inbound stack response");
                let Some(destination) = packet::ip_destination(&response) else { continue; };
                let Some(peer) = operation.device.peer_for_destination(destination) else { continue; };
                let Some(endpoint) = endpoints.get(peer).and_then(|value| *value) else { continue; };
                let packets = packet::fragment_ip_packet(&response, operation.device.mtu() as usize, fragment_id);
                tracing::trace!(fragments = packets.len(), "raw-IP inbound response fragments");
                fragment_id = fragment_id.wrapping_add(1);
                for fragment in packets {
                    match operation.device.send_ip_packet(peer, &fragment) {
                        Ok(actions) => send_network_actions(&socket, endpoint, &actions).await?,
                        Err(error) => tracing::debug!(%error, "raw-IP inbound response encode failed"),
                    }
                }
            }
            _ = tick.tick() => {
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments);
                for (peer, endpoint) in endpoints.iter().enumerate() {
                    let Some(endpoint) = endpoint else { continue; };
                    match operation.device.tick_peer(peer) {
                        Ok(actions) => send_network_actions(&socket, *endpoint, &actions).await?,
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
        }
    };
    tasks.shutdown().await;
    outcome
}

fn refresh_device_generation(
    device: &dyn RawIpInboundDevice,
    generation: &mut u64,
    endpoints: &mut Vec<Option<SocketAddr>>,
    fragments: &mut FragmentReassembler,
) {
    let current = device.generation();
    if *generation != current {
        *generation = current;
        *endpoints = vec![None; device.peer_count()];
        *fragments = FragmentReassembler::new();
    }
}

async fn send_network_actions(
    socket: &zero_platform_tokio::PacketSocket,
    endpoint: SocketAddr,
    actions: &[RawIpInboundAction],
) -> Result<(), EngineError> {
    for action in actions {
        if let RawIpInboundAction::SendNetwork(packet) = action {
            socket
                .send_to(packet, endpoint)
                .await
                .map_err(EngineError::Io)?;
        }
    }
    Ok(())
}

async fn feed_inner_packet(
    packet: &[u8],
    mtu: u16,
    tcp: &UserTcpStack,
    udp: &UserUdpStack,
    responses: &mpsc::Sender<Vec<u8>>,
    echo: &IcmpEchoRelay,
    route: &crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    ingress_id: u64,
    pins: &mut PacketSessionPins,
    fragments: &mut FragmentReassembler,
) {
    let processed = fragments.process(packet, Instant::now());
    let reassembled = matches!(processed, FragmentOutcome::Reassembled(_));
    let packet = match &processed {
        FragmentOutcome::NotFragmented(packet) => *packet,
        FragmentOutcome::Reassembled(packet) => packet.as_slice(),
        FragmentOutcome::Pending => {
            tracing::trace!("raw-IP inbound fragment pending");
            return;
        }
        FragmentOutcome::Rejected(reason) => {
            tracing::trace!(?reason, "raw-IP inbound fragment rejected");
            return;
        }
    };
    tracing::trace!(ip_bytes = packet.len(), protocol = ?packet::ip_protocol(packet), "raw-IP inbound fed packet");
    // A datagram already fragmented to the tunnel MTU is valid after
    // reassembly; its reconstructed length is not a path-MTU violation.
    let effective_mtu = if reassembled {
        usize::from(mtu).max(packet.len())
    } else {
        usize::from(mtu)
    };
    if let Some(response) = packet::build_icmp_mtu_response(packet, effective_mtu) {
        let _ = responses.try_send(response);
        return;
    }
    let Some(destination) = packet::ip_destination(packet) else {
        return;
    };
    let protocol = packet::ip_protocol(packet);
    let candidates = route
        .packet_route_target(destination, protocol)
        .into_candidates();
    for target in candidates {
        match target {
            PacketRouteTarget::Packet { tag, operation } => {
                let generation = route.icmp_egress_generation();
                let plane = PacketPlane::Packet(tag);
                if !pins.permits(packet, &plane) {
                    continue;
                }
                match operation
                    .forward(packet.to_vec(), ingress_id, responses.clone(), generation)
                    .await
                {
                    Ok(Some(response)) => {
                        pins.record(packet, plane);
                        let _ = responses.try_send(response);
                        return;
                    }
                    Ok(None) => {
                        pins.record(packet, plane);
                        return;
                    }
                    Err(error) => tracing::debug!(%error, "packet route candidate unavailable"),
                }
            }
            PacketRouteTarget::Flow => {
                if !pins.permits(packet, &PacketPlane::Flow) {
                    continue;
                }
                pins.record(packet, PacketPlane::Flow);
                match protocol {
                    Some(packet::IPPROTO_TCP) => tcp.feed(packet).await,
                    Some(packet::IPPROTO_UDP) => udp.feed(packet).await,
                    _ => {}
                }
                return;
            }
            PacketRouteTarget::DirectEcho => {
                if !IcmpEchoRelay::accepts_direct_echo(packet) {
                    break;
                }
                if !pins.permits(packet, &PacketPlane::DirectEcho) {
                    continue;
                }
                if echo.dispatch_direct(packet, effective_mtu.min(u16::MAX as usize) as u16) {
                    pins.record(packet, PacketPlane::DirectEcho);
                    return;
                }
                break;
            }
            PacketRouteTarget::Block | PacketRouteTarget::Unsupported => break,
            PacketRouteTarget::Fallback(_) => unreachable!("fallback candidates are flat"),
        }
    }
    if let Some(response) = packet::build_icmp_response(packet, effective_mtu) {
        let _ = responses.try_send(response);
    }
}
