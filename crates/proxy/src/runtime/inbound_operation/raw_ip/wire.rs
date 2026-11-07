//! The real wire driver owns encrypted I/O, timers and endpoint returns only.
//! Raw packets cross the standard Packet session before host routing.
use super::lifecycle::{peers_changed, receive_endpoint_packet, refresh_device_generation};
use super::observation::WireTraffic;
use super::outer::{peer_carrier, refresh_endpoint_peers, send_network_actions, ProxiedWirePacket};
use super::*;
use crate::runtime::raw_ip::timer::{self, PeerTimers};
use std::time::Instant;
use zero_api::TrafficPlane;
use zero_stack::{packet, OwnedFragmentOutcome, OwnedFragmentReassembler};
mod receive;
use receive::{Delivery, ReadMemory};

pub(super) struct Wire {
    pub device: Box<dyn RawIpInboundDevice>,
    pub endpoint: Option<RawIpInboundEndpoint>,
    pub proxies: tasks::RetainedTasks<()>,
}
#[expect(
    clippy::too_many_arguments,
    reason = "the existing raw-IP wire driver borrows each original view and scope explicitly"
)]
pub(super) async fn run(
    mut operation: Wire,
    grant: zero_platform_tokio::contract::component::GrantedDatagramListener,
    reads: [zero_tun::packet_memory::NativeReadGrant; 2],
    control: zero_tun::packet_memory::NativeReadGrant,
    egress: zero_platform_tokio::EgressInterfaceControl,
    packets: mpsc::Sender<zero_core::packet::IpPacket>,
    mut responses: mpsc::Receiver<zero_core::packet::IpPacket>,
    facts: zero_core::packet::PacketFacts,
    observer: Arc<dyn zero_platform_tokio::contract::packet::PacketObserver>,
    mut shutdown: watch::Receiver<bool>,
    checkpoint: zero_platform_tokio::contract::packet::PacketSourceCheckpoint,
) -> Result<(), EngineError> {
    let (mut socket, mut wire_sender) = super::socket::RawIpReceive::split(grant, egress);
    let mut traffic = WireTraffic::new(observer.clone());
    traffic.refresh(operation.device.as_ref());
    #[cfg(test)]
    eprintln!(
        "actual Packet Source wire before proxy consumer lease: {:?}",
        facts.inbound
    );
    let mut proxied_tasks = operation
        .proxies
        .lock()
        .await
        .map_err(std::io::Error::other)?;
    #[cfg(test)]
    eprintln!(
        "actual Packet Source wire got proxy consumer: {:?}",
        facts.inbound
    );
    #[cfg(test)]
    let first_receive_poll = std::sync::Once::new();
    let initial_endpoints = operation
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.peers.borrow().initial_endpoints.clone())
        .unwrap_or_default();
    let mut peer_revision = u64::MAX;
    let mut initial_endpoints = initial_endpoints;
    let mut endpoints = (0..operation.device.peer_count())
        .map(|peer| {
            super::lifecycle::retained_endpoint(operation.device.as_ref(), &initial_endpoints, peer)
        })
        .collect::<Vec<_>>();
    let mut generation = operation.device.generation();
    let (proxied_tx, mut proxied_rx) = mpsc::channel::<ProxiedWirePacket>(256);
    let mut peer_uses_proxy = Vec::new();
    let protocol_memory = zero_tun::packet_memory::PacketDatagramWorkspaces {
        reads: reads.clone(),
        control: Some(control.clone()),
    };
    let read_memory = ReadMemory(reads, control);
    let mut buffer: Option<zero_tun::packet_memory::NativeReadBuffer> = None;
    let mut decode: Option<zero_tun::packet_memory::NativeReadBuffer> = None;
    let mut receiving: Option<
        zero_platform_tokio::contract::BoxFuture<'static, Result<(), EngineError>>,
    > = None;
    let mut fragments = OwnedFragmentReassembler::new();
    let mut endpoint_fragments = OwnedFragmentReassembler::new();
    let mut maintenance = tokio::time::interval(Duration::from_secs(5));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut fragment_id = 1_u32;
    refresh_endpoint_peers(
        operation.endpoint.as_mut(),
        &mut peer_revision,
        &mut initial_endpoints,
        &mut endpoints,
        &mut fragments,
        &mut endpoint_fragments,
        &operation.proxies,
        &mut proxied_tasks,
        &proxied_tx,
        &mut peer_uses_proxy,
    )
    .await?;
    let mut peer_changes = operation
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.peers.clone());
    let mut timers = PeerTimers::default();
    let mut timer_revision = None;
    let outcome = async { loop {
        refresh_endpoint_peers(
            operation.endpoint.as_mut(),
            &mut peer_revision,
            &mut initial_endpoints,
            &mut endpoints,
            &mut fragments,
            &mut endpoint_fragments,
            &operation.proxies,
            &mut proxied_tasks,
            &proxied_tx,
            &mut peer_uses_proxy,
        ).await?;
        refresh_device_generation(
            operation.device.as_ref(),
            &mut generation,
            &mut endpoints,
            &mut fragments,
            &mut endpoint_fragments,
            &initial_endpoints,
        );
        if timer_revision != Some((peer_revision, generation)) {
            traffic.refresh(operation.device.as_ref());
            timers.rebuild(operation.device.peer_count(), |peer| {
                operation.device.timer_schedule(peer)
            });
            timer_revision = Some((peer_revision, generation));
        }
        if *shutdown.borrow() { break Ok(()); }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break Ok(()); }
            }
            request=checkpoint.requested()=>{
                drop(buffer.take());drop(decode.take());
                let mut idle=receiving.is_none() && fragments.pending_datagrams()==0 && endpoint_fragments.pending_datagrams()==0
                    && responses.is_empty() && packets.capacity()==packets.max_capacity() && proxied_rx.is_empty()
                    && proxied_tasks.len()==0 && operation.device.quiescent()
                    && read_memory.0.iter().chain(std::iter::once(&read_memory.1)).all(|read|read.workspace_returned());
                if let Some(endpoint)=&operation.endpoint {
                    idle &= endpoint.packets.is_empty();
                    let devices=endpoint.peers.borrow().devices.clone();
                    for device in devices {
                        let quiet=device.quiescent().await;
                        #[cfg(test)]
                        if !quiet { eprintln!("actual Packet wire stack pause rejected: source={:?} request={} device={}",facts.inbound,request,device.incarnation); }
                        idle &= quiet;
                    }
                }
                #[cfg(test)]
                if !idle { eprintln!("actual Packet wire pause rejected: source={:?} request={} receiving={} fragments={} endpoint_fragments={} responses={} packets_used={} proxied={} proxy_tasks={} protocol={} reads_returned={}",
                    facts.inbound,request,receiving.is_some(),fragments.pending_datagrams(),endpoint_fragments.pending_datagrams(),responses.len(),packets.max_capacity()-packets.capacity(),proxied_rx.len(),proxied_tasks.len(),operation.device.quiescent(),read_memory.0.iter().all(|read|read.workspace_returned())); }
                checkpoint.acknowledge(request,idle);
                tokio::select! {changed=shutdown.changed()=>{if changed.is_err()||*shutdown.borrow(){break Ok(());}},_=checkpoint.resumed(request)=>{}}
            },
            allocated=read_memory.0[0].buffer(),if buffer.is_none()=>{buffer=Some(allocated.map_err(std::io::Error::other)?);},
            changed = peers_changed(&mut peer_changes) => {
                if changed.is_err() { peer_changes = None; }
            }
            credit=read_memory.0[1].buffer(), if decode.is_none() && receiving.is_none() => { decode=Some(credit.map_err(std::io::Error::other)?); }
            delivered=async { receiving.as_mut().expect("same original pending native frame").await }, if receiving.is_some()=> { receiving=None; delivered?; }
            received = async {
                tokio::select! {
                    direct = async { let receive=socket.recv_from(buffer.as_mut().expect("same original read allocation").as_mut_bytes());tokio::pin!(receive);std::future::poll_fn(|cx| { use std::future::Future;let polled=receive.as_mut().poll(cx);if polled.is_pending() || matches!(&polled,std::task::Poll::Ready(Ok(_))) { #[cfg(test)] first_receive_poll.call_once(||eprintln!("actual Packet Source wire first native receive polled: {:?}, pending={}",facts.inbound,polled.is_pending())); observer.ready(); }polled }).await } => direct.map(|(size, source)| (size, Some(source), None, peer_revision)),
                    proxied = proxied_rx.recv() => match proxied {
                        Some(packet) => {
                            let size = packet.bytes.len();
                            if size <= buffer.as_mut().expect("same original read allocation").as_mut_bytes().len() { buffer.as_mut().expect("same original read allocation").as_mut_bytes()[..size].copy_from_slice(&packet.bytes); }
                            Ok((size, packet.source, Some(packet.peer), packet.revision))
                        }
                        None => std::future::pending().await,
                    },
                }
            }, if buffer.is_some() && decode.is_some() && receiving.is_none() => {
                let decode = decode.take().expect("actual decode workspace preceded native receive");
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &operation.proxies, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy).await?;
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                traffic.refresh(operation.device.as_ref());
                if received.is_err() && *shutdown.borrow() { break Ok(()); }
                let (size, source, proxied_peer, received_revision) = received.inspect_err(|_| traffic.aggregate.error(TrafficPlane::Outer, false)).map_err(EngineError::Io)?;
                traffic.aggregate.rx(TrafficPlane::Outer, size, false);
                // Revision stamps belong to queued proxy carriers. A direct
                // socket receive is handled by the current authenticated device;
                // a peer-watch refresh must not discard its first live datagram.
                if size > 65_535 || proxied_peer.is_some() && received_revision != peer_revision { traffic.aggregate.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::InvalidPacket); continue; }
                let original = buffer.take().expect("same original read allocation").into_packet(size)
                    .map_err(|_original|std::io::Error::new(std::io::ErrorKind::InvalidData,"native read exceeded its allocation"))?;
                let output = operation.device.receive_datagram_owned(source, original, decode,
                    control_buffer(&read_memory.1)?, &protocol_memory);
                drop(output.unused_control);
                let dispatch = match output.result {
                    Ok(dispatch) => dispatch,
                    Err(failure) if matches!(failure.storage,Some(zero_traits::PacketDeriveError::Busy | zero_traits::PacketDeriveError::Capacity)) => {
                        traffic.aggregate.dropped_reason(TrafficPlane::Outer,false,zero_api::TrafficDropReason::QueueFull);
                        tracing::debug!(source=?failure.source,"new wire Data was not submitted: transmit capacity"); continue;
                    }
                    Err(failure) if failure.storage.is_none() => {
                        traffic.aggregate.dropped_reason(TrafficPlane::Outer,false,zero_api::TrafficDropReason::InvalidPacket);
                        tracing::debug!(source=?failure.source,"raw-IP protocol rejected wire datagram"); continue;
                    }
                    Err(failure) => return Err(failure.source),
                };
                if let Some(peer) = dispatch.peer_index { timers.update(peer, operation.device.timer_schedule(peer)); }
                if dispatch.authenticated { traffic.peer(dispatch.peer_index).peer_rx(TrafficPlane::Outer, size); }
                traffic.peer(dispatch.peer_index).dropped_count(TrafficPlane::Inner, true, zero_api::TrafficDropReason::SourceRejected, dispatch.source_rejected_packets);
                if proxied_peer.is_some_and(|expected| dispatch.peer_index.is_some_and(|actual| expected != actual)) {
                    traffic.peer(dispatch.peer_index).dropped_reason(TrafficPlane::Outer, dispatch.authenticated, zero_api::TrafficDropReason::SourceRejected);
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
                    send_network_actions(&mut wire_sender, carrier, response_target, &dispatch.actions, &traffic.peer(dispatch.peer_index)).await;
                }
                for action in dispatch.actions {
                    let RawIpInboundAction::ReceiveIp(packet) = action else { continue; };
                    traffic.peer(dispatch.peer_index).rx(TrafficPlane::Inner, packet.len(), true);
                    tracing::trace!(ip_bytes = packet.len(), "raw-IP inbound inner packet");
                    if let Some(peer) = dispatch.peer_index {
                        if let Some(device) = operation.endpoint.as_ref().and_then(|endpoint| endpoint.peers.borrow().devices.get(peer).cloned()) {
                            let (packet, reassembled) = match endpoint_fragments.process((generation,peer),packet, Instant::now()).await {
                                OwnedFragmentOutcome::Packet { packet, reassembled } => (packet, reassembled),
                                OwnedFragmentOutcome::Pending => continue,
                                OwnedFragmentOutcome::Rejected(_) => { traffic.peer(dispatch.peer_index).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::FragmentRejected); continue; },
                            };
                            device.observe_authenticated_packet();
                            let mtu=if reassembled {operation.device.mtu().max(packet.len().min(u16::MAX as usize) as u16)} else {operation.device.mtu()};
                            let local_destination=packet::ip_destination(&packet).is_some_and(|ip|operation.device.is_local_address(ip));
                            receiving=Some(Box::pin(Delivery { packet, device:Some(device), local_destination, mtu, peer:dispatch.peer_index, traffic:traffic.clone(), facts:facts.clone(), packets:packets.clone() }.run()));
                            continue;
                        }
                    }
                    let local_destination=packet::ip_destination(&packet).is_some_and(|ip|operation.device.is_local_address(ip));
                    receiving=Some(Box::pin(Delivery { packet, device:None, local_destination, mtu:operation.device.mtu(), peer:dispatch.peer_index, traffic:traffic.clone(), facts:facts.clone(), packets:packets.clone() }.run()));
                }
            }
            outgoing = receive_endpoint_packet(&mut operation.endpoint) => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &operation.proxies, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy).await?;
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                traffic.refresh(operation.device.as_ref());
                let Some(outgoing) = outgoing else {
                    operation.endpoint = None;
                    peer_changes = None;
                    continue;
                };
                if outgoing.closed.load(std::sync::atomic::Ordering::Acquire) || outgoing.return_channel.as_ref().is_some_and(|channel| channel.is_closed()) { traffic.peer(Some(outgoing.peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::QueueClosed); if let Some(observer) = &outgoing.observer { observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed); } continue; }
                let Some(address) = endpoints.get(outgoing.peer).and_then(|value| *value) else {
                    traffic.peer(Some(outgoing.peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::NoRoute);
                    if let Some(observer) = &outgoing.observer { observer.dropped_reason(zero_traits::PacketDropReason::NoRoute); }
                    continue;
                };
                let carrier = peer_uses_proxy.get(outgoing.peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, outgoing.peer)).flatten();
                let mtu = usize::from(operation.device.mtu());
                let packets=tokio::select! { biased;
                    _=shutdown.changed()=>break Ok(()),
                    result=async move {packet::fragment_ip_packet_prepaid(outgoing.packet,mtu,fragment_id)}=>result.map_err(receive::fragment_failure)?,
                };
                fragment_id = fragment_id.wrapping_add(1);
                for packet in packets {
                    let bytes = packet.len();
                    let output = operation.device.send_ip_packet_owned(outgoing.peer, packet, control_buffer(&read_memory.1)?, &protocol_memory);
                    let actions = owned_result(output).inspect_err(|_| { traffic.peer(Some(outgoing.peer)).error(TrafficPlane::Inner, true); traffic.peer(Some(outgoing.peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); })?;
                    timers.update(outgoing.peer, operation.device.timer_schedule(outgoing.peer));
                    traffic.peer(Some(outgoing.peer)).tx(TrafficPlane::Inner, bytes);
                    if let Some(observer) = &outgoing.observer { observer.sent(bytes); }
                    send_network_actions(&mut wire_sender, carrier.clone(), address, &actions, &traffic.peer(Some(outgoing.peer))).await;
                }
            }
            response = responses.recv() => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &operation.proxies, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy).await?;
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                traffic.refresh(operation.device.as_ref());
                let Some(response) = response else { break if *shutdown.borrow() { Ok(()) } else { Err(EngineError::Io(std::io::Error::other("raw-IP response channel closed"))) }; };
                if response.return_association.endpoint!=facts.endpoint || response.return_association.generation!=facts.inbound.generation {return Err(std::io::Error::other("original Packet response belongs to another source endpoint generation").into());}
                let response=response.into_payload();
                tracing::trace!(ip_bytes = response.len(), "raw-IP inbound stack response");
                let Some(destination) = packet::ip_destination(&response) else { continue; };
                let Some(peer) = operation.device.peer_for_destination(destination) else { continue; };
                let Some(endpoint) = endpoints.get(peer).and_then(|value| *value) else { continue; };
                let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                let packets=tokio::select! { biased;
                    _=shutdown.changed()=>break Ok(()),
                    result=async {packet::fragment_ip_packet_prepaid(response,operation.device.mtu() as usize,fragment_id)}=>result.map_err(receive::fragment_failure)?,
                };
                tracing::trace!(fragments = packets.len(), "raw-IP inbound response fragments");
                fragment_id = fragment_id.wrapping_add(1);
                for fragment in packets {
                    let bytes=fragment.len();
                    let output=operation.device.send_ip_packet_owned(peer,fragment,control_buffer(&read_memory.1)?, &protocol_memory);
                    match owned_result(output) {
                        Ok(actions) => { traffic.respond_packet(bytes); traffic.peer(Some(peer)).tx(TrafficPlane::Inner, bytes); send_network_actions(&mut wire_sender, carrier.clone(), endpoint, &actions, &traffic.peer(Some(peer))).await; },
                        Err(error) => { traffic.peer(Some(peer)).error(TrafficPlane::Inner, true); traffic.peer(Some(peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); tracing::debug!(%error, "raw-IP inbound response encode failed"); },
                    }
                    timers.update(peer, operation.device.timer_schedule(peer));
                }
            }
            _ = timer::wait(timers.deadline()) => {
                // A timer and a replacement notification may become ready in
                // the same select. Refresh the inventory before using old indices.
                if operation.device.generation() != generation || operation.endpoint.as_ref().is_some_and(|endpoint| endpoint.peers.borrow().revision != peer_revision) { continue; }
                // One peer per select turn keeps I/O, control and shutdown fair.
                if let Some(peer) = timers.pop_due() {
                    let result = owned_result(operation.device.tick_peer_owned(peer,control_buffer(&read_memory.1)?, &protocol_memory));
                    let schedule = if result.is_err() {
                        // A failed owner transition must not spin on an overdue
                        // deadline. I/O/config changes can still wake this peer.
                        timer::TimerSchedule::After(Duration::from_millis(250))
                    } else { operation.device.timer_schedule(peer) };
                    timers.update(peer, schedule);
                    match result {
                        Ok(actions) => if let Some(endpoint) = endpoints.get(peer).copied().flatten() {
                            let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                            send_network_actions(&mut wire_sender, carrier, endpoint, &actions, &traffic.peer(Some(peer))).await;
                        } else {
                            for action in actions { if matches!(action, RawIpInboundAction::SendNetwork(_)) { traffic.peer(Some(peer)).dropped_reason(TrafficPlane::Outer, true, zero_api::TrafficDropReason::NoRoute); } }
                        },
                        Err(error) => { traffic.peer(Some(peer)).error(TrafficPlane::Outer, true); tracing::debug!(%error, peer, "raw-IP inbound timer failed"); },
                    }
                }
            }
            _ = maintenance.tick() => {
                let now = Instant::now();
                fragments.cleanup_expired(now);
                endpoint_fragments.cleanup_expired(now);
                traffic.refresh(operation.device.as_ref());
                // Protocol clocks can include system suspend while the executor's
                // clock may not. Reconcile those clocks without running early ticks.
                for peer in 0..operation.device.peer_count() {
                    timers.update(peer, operation.device.timer_schedule(peer));
                }
            }
            joined = proxied_tasks.join_next(), if !proxied_tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    if !error.is_cancelled() { break Err(std::io::Error::other(operation.proxies.remember(error)).into()); }
                }
            }
        }
    } }.await;
    operation.proxies.finish(&mut proxied_tasks).await;
    outcome
}

fn control_buffer(
    grant: &zero_tun::packet_memory::NativeReadGrant,
) -> Result<zero_traits::PacketBuffer, EngineError> {
    grant
        .try_buffer()
        .map_err(std::io::Error::other)?
        .into_packet(grant.bytes())
        .map_err(|_original| {
            std::io::Error::other("actual control workspace length changed").into()
        })
}
fn owned_result<T>(output: crate::runtime::raw_ip::OwnedWireResult<T>) -> Result<T, EngineError> {
    drop(output.unused_control);
    output.result.map_err(|failure| failure.source)
}
