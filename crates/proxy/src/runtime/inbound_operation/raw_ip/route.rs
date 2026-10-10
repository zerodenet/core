//! Routing for decrypted raw IP packets.

use std::time::Instant;
use zero_stack::{packet, FragmentReassembler, OwnedFragmentOutcome, UserTcpStack, UserUdpStack};

use super::IcmpEchoRelay;
use crate::inventory::PacketRouteTarget;
use crate::runtime::packet_route::{PacketPlane, PacketSessionPins};

#[allow(clippy::too_many_arguments)]
pub(super) async fn feed_inner_packet(
    packet: zero_traits::PacketBuffer,
    local_destination: bool,
    traffic: &super::statistics::IngressTraffic,
    peer_identity: Option<std::sync::Arc<str>>,
    mtu: u16,
    tcp: &UserTcpStack,
    udp: &UserUdpStack,
    responses: &zero_stack::packet_output::PacketSender,
    echo: &IcmpEchoRelay,
    route: &crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    ingress_id: u64,
    pins: &mut PacketSessionPins,
    fragments: &mut FragmentReassembler,
) {
    let (mut packet, reassembled) = match fragments.process_buffer(packet, Instant::now()) {
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
    let candidates = route
        .packet_route_target(destination, protocol)
        .into_candidates();
    for target in candidates {
        match target {
            PacketRouteTarget::Packet {
                tag,
                translated,
                operation,
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
