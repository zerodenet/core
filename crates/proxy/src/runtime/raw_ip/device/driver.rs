use super::super::{RawIpAction, RawIpTunnel, RawIpWireCarrier};
use super::{DeviceHealth, ForwardedPackets, PacketReturns};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use zero_api::TrafficPlane;
use zero_engine::EngineError;
use zero_stack::{
    client_udp::ClientUdpStack, packet, ClientTcpStack, FragmentReassembler, OwnedFragmentOutcome,
};

pub(super) struct Device {
    pub(super) traffic: super::super::RawIpTraffic,
    pub(super) endpoint: SocketAddr,
    pub(super) carrier: Arc<dyn RawIpWireCarrier>,
    pub(super) tunnel: Box<dyn RawIpTunnel>,
    pub(super) udp: ClientUdpStack,
    pub(super) tcp: Arc<ClientTcpStack>,
    pub(super) returns: Arc<PacketReturns>,
    pub(super) fragments: FragmentReassembler,
    pub(super) raw_packets: mpsc::Receiver<zero_stack::packet_output::ObservedPacket>,
    pub(super) forwarded_packets: mpsc::Receiver<ForwardedPackets>,
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
    let actions = device
        .tunnel
        .initiate_handshake()
        .inspect_err(|_| device.traffic.error(TrafficPlane::Outer, true))?;
    device.apply(actions).await?;
    device.refresh_handshake_health();
    ready.send_replace(Some(Ok(())));
    let mut cleanup = tokio::time::interval(Duration::from_secs(5));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut returns_sweep = super::maintenance::ReturnMaintenance::new(Instant::now());
    let mut next_tcp_sweep = Instant::now() + Duration::from_secs(60);
    let mut wire = vec![0_u8; 65_535];
    let mut deadline = None;
    loop {
        // Re-evaluate the owner's clock after I/O and maintenance. In particular,
        // an authenticated receive can revive a previously parked protocol.
        deadline = device.tunnel.timer_schedule().deadline(deadline);
        tokio::select! {
            packet = device.raw_packets.recv() => {
                let Some(zero_stack::packet_output::ObservedPacket { packet, observer }) = packet else { return Ok(()); };
                tracing::trace!(ip_bytes = packet.len(), "raw-IP outbound stack packet");
                let packets = if packet::ip_protocol(&packet) == Some(packet::IPPROTO_TCP) {
                    device.tcp.fragment_outbound_packet_owned(packet).await
                } else {
                    vec![packet]
                };
                for packet in packets {
                    let actions = device.tunnel.send_ip_packet(&packet).inspect_err(|_| { device.traffic.error(TrafficPlane::Inner, true); device.traffic.dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::IoFailure); } })?;
                    device.traffic.tx(TrafficPlane::Inner, packet.len());
                    if let Some(observer) = &observer { observer.sent(packet.len()); }
                    device.apply(actions).await?;
                    device.refresh_handshake_health();
                }
            }
            packets = device.forwarded_packets.recv() => {
                let Some(ForwardedPackets { packets, observer, return_channel }) = packets else { return Ok(()); };
                for packet in packets {
                    if return_channel.is_closed() { device.traffic.dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::QueueClosed); if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed); } continue; }
                    let actions = device.tunnel.send_ip_packet(&packet).inspect_err(|_| { device.traffic.error(TrafficPlane::Inner, true); device.traffic.dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::IoFailure); } })?;
                    device.traffic.tx(TrafficPlane::Inner, packet.len());
                    if let Some(observer) = &observer { observer.sent(packet.len()); }
                    device.apply(actions).await?;
                    device.refresh_handshake_health();
                }
            }
            received = device.carrier.recv(&mut wire) => {
                let (size, sender) = received.inspect_err(|_| device.traffic.error(TrafficPlane::Outer, false)).map_err(EngineError::Io)?;
                device.traffic.rx(TrafficPlane::Outer, size, false);
                let Ok((actions, authenticated)) = device.tunnel
                    .receive_datagram_with_authentication(sender, &wire[..size]) else {
                    device.traffic.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::InvalidPacket);
                    continue;
                };
                if authenticated { device.traffic.peer_rx(TrafficPlane::Outer, size); }
                if let Some(sender) = sender {
                    if sender != device.endpoint {
                        if !authenticated || sender.is_ipv4() != device.endpoint.is_ipv4() {
                            device.traffic.dropped_reason(TrafficPlane::Outer, authenticated, zero_api::TrafficDropReason::SourceRejected);
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
            _ = super::super::timer::wait(deadline) => {
                deadline = None;
                let actions = device.tunnel.tick().inspect_err(|_| device.traffic.error(TrafficPlane::Outer, true))?;
                device.apply(actions).await?;
                device.refresh_handshake_health();
            }
            _ = cleanup.tick() => {
                if returns_sweep.due(Instant::now()) { device.returns.expire(); }
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
                        .inspect_err(|_| {
                            self.traffic.error(TrafficPlane::Outer, true);
                            self.traffic.dropped_reason(
                                TrafficPlane::Outer,
                                true,
                                zero_api::TrafficDropReason::IoFailure,
                            );
                        })
                        .map_err(EngineError::Io)?;
                    self.traffic.tx(TrafficPlane::Outer, packet.len());
                }
                RawIpAction::ReceiveIp { packet, source } => {
                    if self.tunnel.allows_source(source) {
                        self.traffic.rx(TrafficPlane::Inner, packet.len(), true);
                        self.health
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .last_authenticated_packet = Some(Instant::now());
                        let packet = match self.fragments.process_buffer(packet, Instant::now()) {
                            OwnedFragmentOutcome::Packet { packet, .. } => packet,
                            OwnedFragmentOutcome::Pending => continue,
                            OwnedFragmentOutcome::Rejected(_) => {
                                self.traffic.dropped_reason(
                                    TrafficPlane::Inner,
                                    true,
                                    zero_api::TrafficDropReason::FragmentRejected,
                                );
                                continue;
                            }
                        };
                        let packet = match self.returns.deliver_owned(packet) {
                            Ok(()) => continue,
                            Err(packet) => packet,
                        };
                        match packet::ip_protocol(&packet) {
                            Some(6) => self.tcp.feed(&packet).await,
                            Some(17) => {
                                self.udp.feed(&packet);
                            }
                            Some(packet::IPPROTO_ICMP) | Some(packet::IPPROTO_ICMPV6) => {
                                if let Some(error) = packet::parse_icmp_error(&packet) {
                                    match error.quoted_protocol {
                                        packet::IPPROTO_TCP => {
                                            self.tcp
                                                .feed_icmp_error_observed(error, Some(packet.len()))
                                                .await;
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
                    } else {
                        self.traffic.dropped_reason(
                            TrafficPlane::Inner,
                            true,
                            zero_api::TrafficDropReason::SourceRejected,
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        // These packets were admitted to Zero but never accepted by the device.
        self.raw_packets.close();
        self.forwarded_packets.close();
        while let Ok(packet) = self.raw_packets.try_recv() {
            self.traffic.dropped_reason(
                TrafficPlane::Inner,
                true,
                zero_api::TrafficDropReason::QueueClosed,
            );
            if let Some(observer) = packet.observer {
                observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
            }
        }
        while let Ok(packets) = self.forwarded_packets.try_recv() {
            for _ in packets.packets {
                self.traffic.dropped_reason(
                    TrafficPlane::Inner,
                    true,
                    zero_api::TrafficDropReason::QueueClosed,
                );
                if let Some(observer) = &packets.observer {
                    observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
                }
            }
        }
    }
}
