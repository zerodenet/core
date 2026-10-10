//! Neutral listener execution and its socket/stack task lifecycle.
use super::lifecycle::{peers_changed, receive_endpoint_packet, refresh_device_generation};
use super::outer::{peer_carrier, refresh_endpoint_peers, send_network_actions, ProxiedWirePacket};
use super::route::feed_inner_packet;
use super::statistics::IngressTraffic;
use super::*;
use crate::runtime::raw_ip::timer::{self, PeerTimers};
use std::time::Instant;
use tokio::task::JoinSet;
use zero_api::TrafficPlane;
use zero_stack::{packet, FragmentReassembler, OwnedFragmentOutcome, UserNetworkStack};
pub(super) async fn run(
    mut operation: RawIpInboundListenerOperation,
    runtime: InboundListenerRuntime,
    socket: zero_platform_tokio::PacketSocket,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError> {
    let mut traffic = IngressTraffic::bind(operation.device.as_ref(), &runtime.route_factory());
    let mtu = operation.device.mtu();
    let (response_tx, mut responses) = mpsc::channel::<zero_traits::PacketBuffer>(256);
    let response_tx: zero_stack::packet_output::PacketSender = response_tx.into();
    let (tcp, udp) = UserNetworkStack::new_with_packet_output(
        response_tx.clone(),
        zero_stack::tcp_mss_for_mtu(mtu),
    )
    .into_parts();
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
        .map(|peer| initial_endpoints.get(peer).copied().flatten())
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
    let ingress_id = crate::runtime::packet_route::next_ingress_id();
    let packet_route = runtime.route_factory();
    let mut packet_pins = packet_route.packet_statistics_pins();
    let mut maintenance = tokio::time::interval(Duration::from_secs(5));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
    let mut peer_changes = operation
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.peers.clone());
    let mut timers = PeerTimers::default();
    let mut timer_revision = None;
    let outcome = loop {
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
        refresh_device_generation(
            operation.device.as_ref(),
            &mut generation,
            &mut endpoints,
            &mut fragments,
            &mut endpoint_fragments,
            &initial_endpoints,
        );
        if timer_revision != Some((peer_revision, generation)) {
            traffic.refresh(operation.device.as_ref(), &packet_route);
            timers.rebuild(operation.device.peer_count(), |peer| {
                operation.device.timer_schedule(peer)
            });
            timer_revision = Some((peer_revision, generation));
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break Ok(()); }
            }
            changed = peers_changed(&mut peer_changes) => {
                if changed.is_err() { peer_changes = None; }
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
                traffic.refresh(operation.device.as_ref(), &packet_route);
                let (size, source, proxied_peer, received_revision) = received.inspect_err(|_| traffic.aggregate.error(TrafficPlane::Outer, false)).map_err(EngineError::Io)?;
                traffic.aggregate.rx(TrafficPlane::Outer, size, false);
                // Revision stamps belong to queued proxy carriers. A direct
                // socket receive is handled by the current authenticated device;
                // a peer-watch refresh must not discard its first live datagram.
                if size > 65_535 || proxied_peer.is_some() && received_revision != peer_revision { traffic.aggregate.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::InvalidPacket); continue; }
                let dispatch = match operation.device.receive_datagram(source, &buffer[..size]) {
                    Ok(dispatch) => dispatch,
                    Err(error) => { traffic.aggregate.dropped_reason(TrafficPlane::Outer, false, zero_api::TrafficDropReason::InvalidPacket); tracing::debug!(%error, "raw-IP inbound rejected datagram"); continue; }
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
                    send_network_actions(&socket, carrier, response_target, &dispatch.actions, traffic.peer(dispatch.peer_index)).await;
                }
                for action in dispatch.actions {
                    let RawIpInboundAction::ReceiveIp(packet) = action else { continue; };
                    traffic.peer(dispatch.peer_index).rx(TrafficPlane::Inner, packet.len(), true);
                    tracing::trace!(ip_bytes = packet.len(), "raw-IP inbound inner packet");
                    if let Some(peer) = dispatch.peer_index {
                        if let Some(device) = operation.endpoint.as_ref().and_then(|endpoint| endpoint.peers.borrow().devices.get(peer).cloned()) {
                            let (packet, reassembled) = match endpoint_fragments.process_buffer(packet, Instant::now()) {
                                OwnedFragmentOutcome::Packet { packet, reassembled } => (packet, reassembled),
                                OwnedFragmentOutcome::Pending => continue,
                                OwnedFragmentOutcome::Rejected(_) => { traffic.peer(dispatch.peer_index).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::FragmentRejected); continue; },
                            };
                            device.observe_authenticated_packet();
                            let inbound_allowed = packet_route.endpoint_inbound_allowed();
                            let local_echo = packet::ip_destination(&packet).is_some_and(|ip| operation.device.is_local_address(ip)) && packet::parse_icmp_echo_request(&packet).is_some();
                            let packet = if local_echo { packet } else {
                                match device.deliver_decrypted_owned(packet, inbound_allowed).await { Ok(()) => continue, Err(packet) => packet }
                            };
                            // Established outbound replies are consumed before
                            // admitting any remote-initiated inner business.
                            if !inbound_allowed { traffic.peer(dispatch.peer_index).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::PolicyRejected); continue; }
                            let mtu = if reassembled {
                                operation.device.mtu().max(packet.len().min(u16::MAX as usize) as u16)
                            } else {
                                operation.device.mtu()
                            };
                            let local_destination = packet::ip_destination(&packet).is_some_and(|ip| operation.device.is_local_address(ip));
                            feed_inner_packet(packet, local_destination, &traffic, traffic.identity(dispatch.peer_index), mtu, &tcp, &udp, &response_tx, &echo, &packet_route, ingress_id, &mut packet_pins, &mut fragments).await;
                            continue;
                        }
                    }
                    if !packet_route.endpoint_inbound_allowed() { traffic.peer(dispatch.peer_index).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::PolicyRejected); continue; }
                    let local_destination = packet::ip_destination(&packet).is_some_and(|ip| operation.device.is_local_address(ip));
                    feed_inner_packet(packet, local_destination, &traffic, traffic.identity(dispatch.peer_index), operation.device.mtu(), &tcp, &udp, &response_tx, &echo, &packet_route, ingress_id, &mut packet_pins, &mut fragments).await;
                }
            }
            outgoing = receive_endpoint_packet(&mut operation.endpoint) => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                traffic.refresh(operation.device.as_ref(), &packet_route);
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
                let packets = packet::fragment_ip_packet_buffer(outgoing.packet, operation.device.mtu() as usize, fragment_id);
                fragment_id = fragment_id.wrapping_add(1);
                for packet in packets {
                    let actions = operation.device.send_ip_packet(outgoing.peer, &packet).inspect_err(|_| { traffic.peer(Some(outgoing.peer)).error(TrafficPlane::Inner, true); traffic.peer(Some(outgoing.peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); })?;
                    timers.update(outgoing.peer, operation.device.timer_schedule(outgoing.peer));
                    traffic.peer(Some(outgoing.peer)).tx(TrafficPlane::Inner, packet.len());
                    if let Some(observer) = &outgoing.observer { observer.sent(packet.len()); }
                    send_network_actions(&socket, carrier.clone(), address, &actions, traffic.peer(Some(outgoing.peer))).await;
                }
            }
            response = responses.recv() => {
                refresh_endpoint_peers(operation.endpoint.as_mut(), &mut peer_revision, &mut initial_endpoints, &mut endpoints, &mut fragments, &mut endpoint_fragments, &mut proxied_tasks, &proxied_tx, &mut peer_uses_proxy);
                refresh_device_generation(operation.device.as_ref(), &mut generation, &mut endpoints, &mut fragments, &mut endpoint_fragments, &initial_endpoints);
                traffic.refresh(operation.device.as_ref(), &packet_route);
                let Some(response) = response else { break Err(EngineError::Io(std::io::Error::other("raw-IP response channel closed"))); };
                tracing::trace!(ip_bytes = response.len(), "raw-IP inbound stack response");
                let Some(destination) = packet::ip_destination(&response) else { continue; };
                let Some(peer) = operation.device.peer_for_destination(destination) else { continue; };
                let Some(endpoint) = endpoints.get(peer).and_then(|value| *value) else { continue; };
                let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                let packets = packet::fragment_ip_packet_buffer(response, operation.device.mtu() as usize, fragment_id);
                tracing::trace!(fragments = packets.len(), "raw-IP inbound response fragments");
                fragment_id = fragment_id.wrapping_add(1);
                for fragment in packets {
                    match operation.device.send_ip_packet(peer, &fragment) {
                        Ok(actions) => { traffic.respond_packet(fragment.len()); traffic.peer(Some(peer)).tx(TrafficPlane::Inner, fragment.len()); send_network_actions(&socket, carrier.clone(), endpoint, &actions, traffic.peer(Some(peer))).await; },
                        Err(error) => { traffic.peer(Some(peer)).error(TrafficPlane::Inner, true); traffic.peer(Some(peer)).dropped_reason(TrafficPlane::Inner, true, zero_api::TrafficDropReason::IoFailure); tracing::debug!(%error, "raw-IP inbound response encode failed"); },
                    }
                    timers.update(peer, operation.device.timer_schedule(peer));
                }
            }
            _ = packet_pins.management_changed() => {
                packet_route.retain_admitted_packet_pins(&mut packet_pins);
                packet_pins.expire();
            },
            _ = timer::wait(timers.deadline()) => {
                // A timer and a replacement notification may become ready in
                // the same select. Refresh the inventory before using old indices.
                if operation.device.generation() != generation || operation.endpoint.as_ref().is_some_and(|endpoint| endpoint.peers.borrow().revision != peer_revision) { continue; }
                // One peer per select turn keeps I/O, control and shutdown fair.
                if let Some(peer) = timers.pop_due() {
                    let result = operation.device.tick_peer(peer);
                    let schedule = if result.is_err() {
                        // A failed owner transition must not spin on an overdue
                        // deadline. I/O/config changes can still wake this peer.
                        timer::TimerSchedule::After(Duration::from_millis(250))
                    } else { operation.device.timer_schedule(peer) };
                    timers.update(peer, schedule);
                    match result {
                        Ok(actions) => if let Some(endpoint) = endpoints.get(peer).copied().flatten() {
                            let carrier = peer_uses_proxy.get(peer).copied().unwrap_or(false).then(|| peer_carrier(&operation.endpoint, peer)).flatten();
                            send_network_actions(&socket, carrier, endpoint, &actions, traffic.peer(Some(peer))).await;
                        } else {
                            for action in actions { if matches!(action, RawIpInboundAction::SendNetwork(_)) { traffic.peer(Some(peer)).dropped_reason(TrafficPlane::Outer, true, zero_api::TrafficDropReason::NoRoute); } }
                        },
                        Err(error) => { traffic.peer(Some(peer)).error(TrafficPlane::Outer, true); tracing::debug!(%error, peer, "raw-IP inbound timer failed"); },
                    }
                }
            }
            _ = maintenance.tick() => {
                packet_route.retain_admitted_packet_pins(&mut packet_pins);
                traffic.refresh(operation.device.as_ref(), &packet_route);
                // Protocol clocks can include system suspend while the executor's
                // clock may not. Reconcile those clocks without running early ticks.
                for peer in 0..operation.device.peer_count() {
                    timers.update(peer, operation.device.timer_schedule(peer));
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
