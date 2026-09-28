use super::super::{RawIpAction, RawIpTunnel, RawIpWireCarrier};
use super::{DeviceHealth, PacketReturns};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use zero_engine::EngineError;
use zero_stack::{
    client_udp::ClientUdpStack, packet, ClientTcpStack, FragmentOutcome, FragmentReassembler,
};

pub(super) struct Device {
    pub(super) endpoint: SocketAddr,
    pub(super) carrier: Arc<dyn RawIpWireCarrier>,
    pub(super) tunnel: Box<dyn RawIpTunnel>,
    pub(super) udp: ClientUdpStack,
    pub(super) tcp: Arc<ClientTcpStack>,
    pub(super) returns: Arc<PacketReturns>,
    pub(super) fragments: FragmentReassembler,
    pub(super) raw_packets: mpsc::Receiver<Vec<u8>>,
    pub(super) forwarded_packets: mpsc::Receiver<Vec<Vec<u8>>>,
    pub(super) closed: Arc<AtomicBool>,
    pub(super) health: Arc<Mutex<DeviceHealth>>,
}

pub(super) async fn run_device(
    mut device: Device,
    ready: watch::Sender<Option<Result<(), String>>>,
) {
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
            received = device.carrier.recv(&mut wire) => {
                let (size, sender) = received.map_err(EngineError::Io)?;
                let Ok((actions, authenticated)) = device.tunnel
                    .receive_datagram_with_authentication(sender, &wire[..size]) else {
                    continue;
                };
                if let Some(sender) = sender {
                    if sender != device.endpoint {
                        if !authenticated || sender.is_ipv4() != device.endpoint.is_ipv4() {
                            continue;
                        }
                        tracing::debug!(old_endpoint = %device.endpoint, new_endpoint = %sender, "raw-IP peer endpoint roamed");
                        device.endpoint = sender;
                    }
                }
                tracing::trace!(wire_bytes = size, actions = actions.len(), "raw-IP outbound datagram processed");
                device.apply(actions).await?;
                device.refresh_handshake_health();
            }
            _ = timer.tick() => {
                device.returns.expire();
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
                    self.carrier
                        .send(&packet, self.endpoint)
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
