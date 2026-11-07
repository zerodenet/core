//! The existing standalone wire driver consumes its unique prepaid native IO.
use super::*;
mod prepared;
use zero_stack::OwnedFragmentReassembler;
use zero_tun::packet_memory::{NativeReadBuffer, NativeReadGrant};

pub(super) async fn run(
    device: &mut Device,
    ready: &watch::Sender<Option<Result<(), String>>>,
    reads: super::super::wire::WireReads,
    checkpoint: crate::runtime::packet_route::host::pause::Checkpoint,
) -> Result<(), EngineError> {
    let output = device
        .tunnel
        .initiate_handshake_owned(control(&reads.control)?, &reads.memory());
    let actions = owned(output).inspect_err(|_| device.traffic.error(TrafficPlane::Outer, true))?;
    network(device, actions).await?;
    device.refresh_handshake_health();
    ready.send_replace(Some(Ok(())));
    let mut cleanup = tokio::time::interval(Duration::from_secs(5));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut returns_sweep = super::super::maintenance::ReturnMaintenance::new(Instant::now());
    let mut next_tcp_sweep = Instant::now() + Duration::from_secs(60);
    let mut buffer: Option<NativeReadBuffer> = None;
    let mut decode: Option<NativeReadBuffer> = None;
    let mut fragments = OwnedFragmentReassembler::new();
    let mut receiving: Option<
        zero_platform_tokio::contract::BoxFuture<'static, Result<(), EngineError>>,
    > = None;
    let mut deadline = None;
    loop {
        deadline = device.tunnel.timer_schedule().deadline(deadline);
        tokio::select! {
            request = checkpoint.requested() => {
                drop(buffer.take());
                drop(decode.take());
                let idle = receiving.is_none() && fragments.pending_datagrams() == 0
                    && device.raw_packets.is_empty() && device.forwarded_packets.is_empty()
                    && device.returns.quiescent() && device.tunnel.quiescent()
                    && device.users.as_ref().is_some_and(|users| users.quiescent())
                    && device.udp.quiescent() && device.tcp.quiescent().await
                    && reads.reads.iter().chain(std::iter::once(&reads.control)).all(|grant| grant.workspace_returned());
                checkpoint.acknowledge(request, if idle { Ok(()) } else {
                    Err(zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::Capacity,
                        zero_platform_tokio::contract::Impact::Component,
                    ))
                });
                checkpoint.resumed(request).await;
            }
            allocated = reads.reads[0].buffer(), if buffer.is_none() => {
                buffer = Some(allocated.map_err(std::io::Error::other)?);
            }
            allocated = reads.reads[1].buffer(), if decode.is_none() && receiving.is_none() => {
                decode = Some(allocated.map_err(std::io::Error::other)?);
            }
            delivered = async { receiving.as_mut().expect("same parked native frame").await }, if receiving.is_some() => {
                receiving = None;
                delivered?;
            }
            packet = device.raw_packets.recv() => {
                let Some(zero_stack::packet_output::ObservedPacket { packet, observer }) = packet else { return Ok(()); };
                let packets = if packet::ip_protocol(&packet) == Some(packet::IPPROTO_TCP) {
                    device.tcp.fragment_outbound_packet_buffer(packet).await.map_err(std::io::Error::other)?
                } else { vec![packet] };
                for packet in packets {
                    send_inner(device, packet, observer.as_deref(), &reads).await?;
                }
            }
            packets = device.forwarded_packets.recv() => {
                let Some(ForwardedPackets { packets, observer, return_channel }) = packets else { return Ok(()); };
                for packet in packets {
                    if return_channel.is_closed() {
                        device.traffic.dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::QueueClosed);
                        if let Some(observer) = &observer { observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed); }
                        continue;
                    }
                    send_inner(device, packet, observer.as_deref(), &reads).await?;
                }
            }
            received = async { device.carrier.recv(buffer.as_mut().expect("same prepaid wire read").as_mut_bytes()).await },
                if buffer.is_some() && decode.is_some() && receiving.is_none() => {
                let (size, sender) = received.inspect_err(|_| device.traffic.error(TrafficPlane::Outer, false))?;
                let decode = decode.take().expect("decode workspace preceded original receive");
                let original = buffer.take().expect("same prepaid wire read").into_packet(size)
                    .map_err(|_original| std::io::Error::new(std::io::ErrorKind::InvalidData, "wire read exceeded its actual allocation"))?;
                device.traffic.rx(TrafficPlane::Outer, size, false);
                let output = device.tunnel.prepare_datagram_owned(sender, original, decode, control(&reads.control)?, &reads.memory());
                // Only the protocol can report a capacity refusal before SDK
                // submission/authentication/replay commitment. Actual queued
                // or admitted inner returns are never evicted by this branch.
                let unused = output.unused_control;
                let result = output.result;
                drop(unused);
                let received = match result {
                    Ok(received) => received,
                    Err(failure) if matches!(failure.storage,
                        Some(zero_traits::PacketDeriveError::Busy | zero_traits::PacketDeriveError::Capacity)) => {
                        device.traffic.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::QueueFull);
                        tracing::debug!(source=?failure.source, "wire Data not submitted: packet transmit capacity");
                        continue;
                    }
                    Err(failure) if failure.storage.is_none() => {
                        device.traffic.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::InvalidPacket);
                        tracing::debug!(source=?failure.source, "wire datagram rejected by original protocol");
                        continue;
                    }
                    Err(failure) => return Err(failure.source),
                };
                let Some((actions,authenticated))=prepared::finish(device,received,sender,&reads.reads[0]).await? else {continue;};
                if authenticated { device.traffic.peer_rx(TrafficPlane::Outer, size); }
                if let Some(sender) = sender.filter(|sender| *sender != device.endpoint) {
                    if !authenticated || sender.is_ipv4() != device.endpoint.is_ipv4() {
                        device.traffic.dropped_reason(TrafficPlane::Outer, authenticated, zero_api::TrafficDropReason::SourceRejected);
                        continue;
                    }
                    device.endpoint = sender;
                }
                if let Some(packet) = receive_inner(device, actions, &mut fragments).await? {
                    receiving = Some(Box::pin(Delivery {
                        packet, returns: device.returns.clone(), tcp: device.tcp.clone(), udp: device.udp.clone(),
                    }.run()));
                }
                device.refresh_handshake_health();
            }
            _ = super::super::super::timer::wait(deadline) => {
                deadline = None;
                let output = device.tunnel.tick_owned(control(&reads.control)?, &reads.memory());
                let actions = owned(output).inspect_err(|_| device.traffic.error(TrafficPlane::Outer, true))?;
                network(device, actions).await?;
                device.refresh_handshake_health();
            }
            _ = cleanup.tick() => {
                let now = Instant::now();
                fragments.cleanup_expired(now);
                device.udp.cleanup_expired(now);
                if returns_sweep.due(now) { device.returns.expire(); }
                if now >= next_tcp_sweep {
                    device.tcp.cleanup_idle(Duration::from_secs(600)).await;
                    next_tcp_sweep = Instant::now() + Duration::from_secs(60);
                }
            }
        }
    }
}

async fn send_inner(
    device: &mut Device,
    packet: zero_traits::PacketBuffer,
    observer: Option<&dyn zero_traits::IoObserver>,
    reads: &super::super::wire::WireReads,
) -> Result<(), EngineError> {
    let length = packet.len();
    let output =
        device
            .tunnel
            .send_ip_packet_owned(packet, control(&reads.control)?, &reads.memory());
    let actions = owned(output).inspect_err(|_| {
        device.traffic.error(TrafficPlane::Inner, true);
        device.traffic.dropped_reason(
            TrafficPlane::Inner,
            true,
            zero_api::TrafficDropReason::IoFailure,
        );
        if let Some(observer) = observer {
            observer.dropped_reason(zero_traits::PacketDropReason::IoFailure);
        }
    })?;
    device.traffic.tx(TrafficPlane::Inner, length);
    if let Some(observer) = observer {
        observer.sent(length);
    }
    network(device, actions).await?;
    device.refresh_handshake_health();
    Ok(())
}
async fn network(device: &mut Device, actions: Vec<RawIpAction>) -> Result<(), EngineError> {
    for action in actions {
        let RawIpAction::SendNetwork(packet) = action else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "standalone tunnel produced an inner receive outside its prepaid native receive",
            )
            .into());
        };
        device
            .carrier
            .send(&packet, device.endpoint)
            .await
            .inspect_err(|_| {
                device.traffic.error(TrafficPlane::Outer, true);
                device.traffic.dropped_reason(
                    TrafficPlane::Outer,
                    true,
                    zero_api::TrafficDropReason::IoFailure,
                );
            })?;
        device.traffic.tx(TrafficPlane::Outer, packet.len());
    }
    Ok(())
}
async fn receive_inner(
    device: &mut Device,
    actions: Vec<RawIpAction>,
    fragments: &mut OwnedFragmentReassembler<()>,
) -> Result<Option<zero_traits::PacketBuffer>, EngineError> {
    let mut receiving = None;
    for action in actions {
        match action {
            RawIpAction::SendNetwork(packet) => {
                network(device, vec![RawIpAction::SendNetwork(packet)]).await?
            }
            RawIpAction::ReceiveIp { packet, source } => {
                if !device.tunnel.allows_source(source) {
                    device.traffic.dropped_reason(
                        TrafficPlane::Inner,
                        true,
                        zero_api::TrafficDropReason::SourceRejected,
                    );
                    continue;
                }
                device.traffic.rx(TrafficPlane::Inner, packet.len(), true);
                device
                    .health
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .last_authenticated_packet = Some(Instant::now());
                // This driver is one exact authenticated peer/session. Its
                // private cache has no cross-peer or latest-source lookup.
                match fragments.process((), packet, Instant::now()).await {
                    OwnedFragmentOutcome::Packet { packet, .. } => receiving = Some(packet),
                    OwnedFragmentOutcome::Pending => {}
                    OwnedFragmentOutcome::Rejected(_) => device.traffic.dropped_reason(
                        TrafficPlane::Inner,
                        true,
                        zero_api::TrafficDropReason::FragmentRejected,
                    ),
                }
            }
        }
    }
    Ok(receiving)
}

struct Delivery {
    packet: zero_traits::PacketBuffer,
    returns: Arc<PacketReturns>,
    tcp: Arc<ClientTcpStack>,
    udp: ClientUdpStack,
}
impl Delivery {
    async fn run(self) -> Result<(), EngineError> {
        let Some(packet) = self
            .returns
            .deliver_owned_wait(self.packet)
            .await
            .map_err(std::io::Error::other)?
        else {
            return Ok(());
        };
        match packet::ip_protocol(&packet) {
            Some(packet::IPPROTO_TCP) => self.tcp.feed(&packet).await,
            Some(packet::IPPROTO_UDP) => {
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
        Ok(())
    }
}

fn control(grant: &NativeReadGrant) -> Result<zero_traits::PacketBuffer, EngineError> {
    grant
        .try_buffer()
        .map_err(std::io::Error::other)?
        .into_packet(grant.bytes())
        .map_err(|_buffer| std::io::Error::other("actual control workspace length changed").into())
}
fn owned<T>(output: crate::runtime::raw_ip::OwnedWireResult<T>) -> Result<T, EngineError> {
    // Operation already returned; no protocol/device lock covers credit wake.
    drop(output.unused_control);
    output.result.map_err(|failure| failure.source)
}
