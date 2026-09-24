//! Shared raw-IP peer device for active TCP and UDP stacks.

mod returns;
#[cfg(test)]
mod tests;

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use tokio::{
    sync::{mpsc, watch},
    task::AbortHandle,
};
use zero_api::{OutboundDeviceHealthSnapshot, OutboundDeviceHealthState};
use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;
use zero_stack::{
    client_udp::ClientUdpStack, packet, ClientTcpStack, FragmentOutcome, FragmentReassembler,
    UserTcpStream,
};

use super::{RawIpAction, RawIpTunnel};
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
                socket,
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

struct Device {
    endpoint: SocketAddr,
    socket: TokioDatagramSocket,
    tunnel: Box<dyn RawIpTunnel>,
    udp: ClientUdpStack,
    tcp: Arc<ClientTcpStack>,
    returns: Arc<PacketReturns>,
    fragments: FragmentReassembler,
    raw_packets: mpsc::Receiver<Vec<u8>>,
    forwarded_packets: mpsc::Receiver<Vec<Vec<u8>>>,
    closed: Arc<AtomicBool>,
    health: Arc<Mutex<DeviceHealth>>,
}

#[derive(Default)]
struct DeviceHealth {
    last_handshake: Option<Instant>,
    last_authenticated_packet: Option<Instant>,
}

impl DeviceHealth {
    fn snapshot(
        &self,
        tag: String,
        peer_index: usize,
        closed: bool,
    ) -> OutboundDeviceHealthSnapshot {
        const FRESH: Duration = Duration::from_secs(180);
        let handshake_age = self.last_handshake.map(|time| time.elapsed());
        let packet_age = self.last_authenticated_packet.map(|time| time.elapsed());
        let state = if closed {
            OutboundDeviceHealthState::Stopped
        } else if handshake_age.is_none() {
            OutboundDeviceHealthState::AwaitingHandshake
        } else if self.last_authenticated_packet.is_some_and(|packet| {
            self.last_handshake
                .is_some_and(|handshake| packet >= handshake)
                && packet_age.is_some_and(|age| age <= FRESH)
        }) {
            OutboundDeviceHealthState::Reachable
        } else if handshake_age.is_some_and(|age| age > FRESH) {
            OutboundDeviceHealthState::Degraded
        } else {
            OutboundDeviceHealthState::RecentlyHandshaken
        };
        OutboundDeviceHealthSnapshot {
            tag,
            peer_index,
            state,
            last_handshake_age_ms: handshake_age.map(|age| age.as_millis() as u64),
            last_authenticated_packet_age_ms: packet_age.map(|age| age.as_millis() as u64),
            endpoint_resolution_failed: false,
        }
    }
}

async fn run_device(mut device: Device, ready: watch::Sender<Option<Result<(), String>>>) {
    let result = run_device_inner(&mut device, &ready).await;
    if let Err(error) = result {
        ready.send_replace(Some(Err(error.to_string())));
        tracing::warn!(error = %error, "raw-IP device stopped");
    }
    device.closed.store(true, Ordering::Release);
}

async fn run_device_inner(
    device: &mut Device,
    ready: &watch::Sender<Option<Result<(), String>>>,
) -> Result<(), EngineError> {
    let actions = device.tunnel.initiate_handshake()?;
    device.apply(actions).await?;
    device.refresh_handshake_health();
    ready.send_replace(Some(Ok(())));
    let mut timer = tokio::time::interval(Duration::from_millis(250));
    let mut next_tcp_sweep = Instant::now() + Duration::from_secs(60);
    let mut wire = vec![0_u8; 65_535];
    loop {
        tokio::select! {
            packet = device.raw_packets.recv() => {
                let Some(packet) = packet else { return Ok(()); };
                tracing::trace!(ip_bytes = packet.len(), "raw-IP outbound stack packet");
                let packets = if packet::ip_protocol(&packet) == Some(packet::IPPROTO_TCP) {
                    device.tcp.fragment_outbound_packet(&packet).await
                } else {
                    vec![packet]
                };
                for packet in packets {
                    let actions = device.tunnel.send_ip_packet(&packet)?;
                    device.apply(actions).await?;
                    device.refresh_handshake_health();
                }
            }
            packets = device.forwarded_packets.recv() => {
                let Some(packets) = packets else { return Ok(()); };
                for packet in packets {
                    let actions = device.tunnel.send_ip_packet(&packet)?;
                    device.apply(actions).await?;
                    device.refresh_handshake_health();
                }
            }
            received = device.socket.recv_from_addr(&mut wire) => {
                let (size, sender) = received.map_err(EngineError::Io)?;
                let Ok((actions, authenticated)) = device.tunnel
                    .receive_datagram_with_authentication(Some(sender.ip()), &wire[..size]) else {
                    continue;
                };
                if sender != device.endpoint {
                    if !authenticated || sender.is_ipv4() != device.endpoint.is_ipv4() {
                        continue;
                    }
                    tracing::debug!(old_endpoint = %device.endpoint, new_endpoint = %sender, "raw-IP peer endpoint roamed");
                    device.endpoint = sender;
                }
                tracing::trace!(wire_bytes = size, actions = actions.len(), "raw-IP outbound datagram processed");
                device.apply(actions).await?;
                device.refresh_handshake_health();
            }
            _ = timer.tick() => {
                let actions = device.tunnel.tick()?;
                device.apply(actions).await?;
                device.refresh_handshake_health();
                if Instant::now() >= next_tcp_sweep {
                    device.tcp.cleanup_idle(Duration::from_secs(600)).await;
                    next_tcp_sweep = Instant::now() + Duration::from_secs(60);
                }
            }
        }
    }
}

impl Device {
    fn refresh_handshake_health(&self) {
        if let Some(age) = self.tunnel.time_since_last_handshake() {
            let now = Instant::now();
            self.health
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .last_handshake = now.checked_sub(age);
        }
    }

    async fn apply(&mut self, actions: Vec<RawIpAction>) -> Result<(), EngineError> {
        for action in actions {
            match action {
                RawIpAction::SendNetwork(packet) => {
                    self.socket
                        .send_to_addr(&packet, self.endpoint)
                        .await
                        .map_err(EngineError::Io)?;
                }
                RawIpAction::ReceiveIp { packet, source } => {
                    if self.tunnel.allows_source(source) {
                        self.health
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .last_authenticated_packet = Some(Instant::now());
                        let packet = match self.fragments.process(&packet, Instant::now()) {
                            FragmentOutcome::NotFragmented(packet) => packet.to_vec(),
                            FragmentOutcome::Reassembled(packet) => packet,
                            FragmentOutcome::Pending | FragmentOutcome::Rejected(_) => continue,
                        };
                        match packet::ip_protocol(&packet) {
                            _ if self.returns.deliver(&packet) => {}
                            Some(6) => self.tcp.feed(&packet).await,
                            Some(17) => {
                                self.udp.feed(&packet);
                            }
                            Some(packet::IPPROTO_ICMP) | Some(packet::IPPROTO_ICMPV6) => {
                                if let Some(error) = packet::parse_icmp_error(&packet) {
                                    match error.quoted_protocol {
                                        packet::IPPROTO_TCP => {
                                            self.tcp.feed_icmp_error(error).await;
                                        }
                                        packet::IPPROTO_UDP => {
                                            self.udp.feed(&packet);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
