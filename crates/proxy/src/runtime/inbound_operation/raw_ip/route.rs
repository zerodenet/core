//! Routing for decrypted raw IP packets.

use std::time::Instant;
use zero_stack::{
    OwnedFragmentOutcome, OwnedFragmentReassembler, UserTcpStack, UserUdpStack, packet,
};

use super::IcmpEchoRelay;
pub(super) type FragmentNamespace = (
    zero_core::component::EndpointId,
    zero_core::component::ResourceGeneration,
    Option<zero_core::component::PeerId>,
    u64,
);
use crate::inventory::PacketRouteTarget;
use crate::runtime::packet_route::{PacketPlane, PacketSessionPins};

#[allow(clippy::too_many_arguments)]
pub(super) async fn feed_inner_packet(
    original: zero_core::packet::IpPacket,
    facts: std::sync::Arc<zero_core::packet::PacketFacts>,
    source: &crate::runtime::inbound_owners::InboundTaskAccess,
    packet_roles: &crate::runtime::packet_route::PacketRoleSessions,
    local_destination: bool,
    traffic: &super::statistics::IngressTraffic,
    peer_identity: Option<std::sync::Arc<str>>,
    mtu: u16,
    return_mtu: u16,
    tcp: &UserTcpStack,
    udp: &UserUdpStack,
    responses: &zero_stack::packet_output::PacketSender,
    echo: &IcmpEchoRelay,
    route: &crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    ingress_id: u64,
    pins: &mut PacketSessionPins,
    fragments: &mut OwnedFragmentReassembler<FragmentNamespace>,
    tun: Option<&crate::inbound::tun::TunIngressOptions>,
) {
    if route.ensure_configuration_committed().is_err() {
        traffic.drop_inner(
            peer_identity.as_deref(),
            zero_api::TrafficDropReason::PolicyRejected,
        );
        return;
    }
    let association = original.return_association.clone();
    let ingress = original.ingress();
    let (mut packet, reassembled) = match fragments
        .process(
            (
                association.endpoint,
                association.generation,
                association.peer.clone(),
                association.correlation,
            ),
            original.into_payload(),
            Instant::now(),
        )
        .await
    {
        OwnedFragmentOutcome::Packet {
            packet,
            reassembled,
        } => (packet, reassembled),
        OwnedFragmentOutcome::Pending => return,
        OwnedFragmentOutcome::Rejected(reason) => {
            traffic.drop_inner(
                peer_identity.as_deref(),
                zero_api::TrafficDropReason::FragmentRejected,
            );
            tracing::trace!(?reason, "raw-IP inbound fragment rejected");
            return;
        }
    };
    let packet_key = packet::packet_conversation_key(&packet);
    traffic.admit_packet(packet.len());
    tracing::trace!(ip_bytes = packet.len(), protocol = ?packet::ip_protocol(&packet), "raw-IP inbound fed packet");
    // A datagram already fragmented to the tunnel MTU is valid after
    // reassembly; its reconstructed length is not a path-MTU violation.
    let effective_mtu = if reassembled {
        usize::from(mtu).max(packet.len())
    } else {
        usize::from(mtu)
    };
    if let Some(response) = packet::build_icmp_mtu_response(&packet, effective_mtu) {
        traffic.drop_inner(
            peer_identity.as_deref(),
            zero_api::TrafficDropReason::FragmentRejected,
        );
        traffic.send_response(responses, response, peer_identity.as_deref());
        return;
    }
    let Some(destination) = packet::ip_destination(&packet) else {
        traffic.drop_inner(
            peer_identity.as_deref(),
            zero_api::TrafficDropReason::InvalidPacket,
        );
        return;
    };
    let protocol = packet::ip_protocol(&packet);
    if local_destination && packet::parse_icmp_echo_request(&packet).is_some() {
        if let Some(response) = packet::build_local_icmp_echo_reply(&packet, effective_mtu) {
            traffic.send_response(responses, response, peer_identity.as_deref());
        } else {
            traffic.drop_inner(
                peer_identity.as_deref(),
                zero_api::TrafficDropReason::InvalidPacket,
            );
        }
        return;
    }
    // TUN intercepts DNS before ordinary Packet routing. Assigned addresses
    // and MTU were supplied by the same actual Source original.
    if tun.is_some_and(|policy| policy.dns_hijack)
        && (packet::parse_tcp(&packet).is_some_and(|tcp| tcp.dst.port == 53)
            || packet::parse_udp(&packet).is_some_and(|udp| udp.dst.port == 53))
    {
        match protocol {
            Some(packet::IPPROTO_TCP) => tcp.feed_with_peer(&packet, peer_identity).await,
            Some(packet::IPPROTO_UDP) => udp.feed_with_peer(&packet, peer_identity).await,
            _ => {}
        }
        return;
    }
    let candidates = route
        .packet_route_target(destination, protocol)
        .into_candidates();
    for target in candidates {
        match target {
            PacketRouteTarget::Packet {
                tag,
                translated,
                operation,
                role,
                ..
            } => {
                let generation = route.icmp_egress_generation();
                let plane = if translated {
                    PacketPlane::TranslatedPacket(tag.clone())
                } else {
                    PacketPlane::Packet(tag.clone())
                };
                if !pins.permits_peer(&packet, &plane, peer_identity.clone()) {
                    continue;
                }
                let observer = pins.inner_io(&packet, &plane, peer_identity.clone(), || {
                    route.outbound_inner_io(&tag)
                });
                if let Some(role) = role {
                    let frame = zero_core::packet::IpPacket::new(
                        std::mem::take(&mut packet),
                        association.clone(),
                    )
                    .expect("validated original or actual reassembly");
                    let frame = match ingress {
                        Some(ingress) => frame.with_ingress(ingress),
                        None => frame,
                    };
                    if !pins.admit_standard(&frame, &plane, peer_identity.clone()) {
                        packet = frame.into_payload();
                        continue;
                    }
                    match packet_roles
                        .send(
                            role,
                            translated,
                            facts.clone(),
                            return_mtu,
                            frame,
                            route,
                            source,
                            observer.clone(),
                        )
                        .await
                    {
                        Ok(selected_peer) => {
                            pins.record_observed_key(
                                packet_key,
                                plane,
                                peer_identity.clone(),
                                selected_peer,
                                observer,
                            );
                            return;
                        }
                        Err(failure) => {
                            pins.reject_unaccepted_key(packet_key, peer_identity.clone());
                            let retry = failure.failure.delivery
                                == zero_platform_tokio::contract::DeliveryState::NotSubmitted;
                            if let Some(original) = failure.packet {
                                packet = original.into_payload();
                            }
                            tracing::debug!(error=%failure.failure, "actual Packet role candidate unavailable");
                            if !retry || packet.is_empty() {
                                break;
                            }
                            continue;
                        }
                    }
                }
                let Some(replies) =
                    pins.replies_for(&packet, &plane, peer_identity.clone(), responses.clone())
                else {
                    continue;
                };
                match operation
                    .forward(
                        &mut packet,
                        ingress_id,
                        replies,
                        generation,
                        observer.clone(),
                    )
                    .await
                {
                    Ok(observed) => {
                        pins.record_observed_key(
                            packet_key,
                            plane,
                            peer_identity.clone(),
                            observed.peer_identity,
                            observer,
                        );
                        if let Some(response) = observed.response {
                            traffic.send_response(responses, response, peer_identity.as_deref());
                        }
                        return;
                    }
                    Err(error) => {
                        pins.reject_unaccepted_key(packet_key, peer_identity.clone());
                        if packet.is_empty() {
                            break;
                        }
                        tracing::debug!(%error, "packet route candidate unavailable");
                    }
                }
            }
            PacketRouteTarget::Flow => {
                if !pins.permits_peer(&packet, &PacketPlane::Flow, peer_identity.clone()) {
                    continue;
                }
                pins.record_peers(&packet, PacketPlane::Flow, peer_identity.clone(), None);
                match protocol {
                    Some(packet::IPPROTO_TCP) => {
                        tcp.feed_with_peer(&packet, peer_identity.clone()).await
                    }
                    Some(packet::IPPROTO_UDP) => {
                        udp.feed_with_peer(&packet, peer_identity.clone()).await
                    }
                    _ => {}
                }
                return;
            }
            PacketRouteTarget::DirectEcho { dial_policy } => {
                if dial_policy != zero_traits::DialPolicy::default() {
                    break;
                }
                if !IcmpEchoRelay::accepts_direct_echo(&packet) {
                    break;
                }
                if !pins.permits_peer(&packet, &PacketPlane::DirectEcho, peer_identity.clone()) {
                    continue;
                }
                if echo.dispatch_direct(&packet, effective_mtu.min(u16::MAX as usize) as u16) {
                    pins.record_peers(
                        &packet,
                        PacketPlane::DirectEcho,
                        peer_identity.clone(),
                        None,
                    );
                    return;
                }
                break;
            }
            PacketRouteTarget::Block => {
                traffic.drop_inner(
                    peer_identity.as_deref(),
                    zero_api::TrafficDropReason::PolicyRejected,
                );
                if let Some(response) = packet::build_icmp_response(&packet, effective_mtu) {
                    traffic.send_response(responses, response, peer_identity.as_deref());
                }
                return;
            }
            PacketRouteTarget::Unsupported => break,
            PacketRouteTarget::Fallback(_) => unreachable!("fallback candidates are flat"),
        }
    }
    traffic.drop_inner(
        peer_identity.as_deref(),
        zero_api::TrafficDropReason::NoRoute,
    );
    if let Some(response) = packet::build_icmp_response(&packet, effective_mtu) {
        traffic.send_response(responses, response, peer_identity.as_deref());
    }
}
