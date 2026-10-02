//! Routing for decrypted raw IP packets.

use std::time::Instant;
use tokio::sync::mpsc;
use zero_stack::{packet, FragmentOutcome, FragmentReassembler, UserTcpStack, UserUdpStack};

use super::IcmpEchoRelay;
use crate::inventory::PacketRouteTarget;
use crate::runtime::packet_route::{PacketPlane, PacketSessionPins};

#[allow(clippy::too_many_arguments)]
pub(super) async fn feed_inner_packet(
    packet: &[u8],
    traffic: &super::statistics::IngressTraffic,
    peer_identity: Option<std::sync::Arc<str>>,
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
            traffic.drop_inner(
                peer_identity.as_deref(),
                zero_api::TrafficDropReason::FragmentRejected,
            );
            tracing::trace!(?reason, "raw-IP inbound fragment rejected");
            return;
        }
    };
    traffic.admit_packet(packet.len());
    tracing::trace!(ip_bytes = packet.len(), protocol = ?packet::ip_protocol(packet), "raw-IP inbound fed packet");
    // A datagram already fragmented to the tunnel MTU is valid after
    // reassembly; its reconstructed length is not a path-MTU violation.
    let effective_mtu = if reassembled {
        usize::from(mtu).max(packet.len())
    } else {
        usize::from(mtu)
    };
    if let Some(response) = packet::build_icmp_mtu_response(packet, effective_mtu) {
        traffic.drop_inner(
            peer_identity.as_deref(),
            zero_api::TrafficDropReason::FragmentRejected,
        );
        traffic.send_response(responses, response, peer_identity.as_deref());
        return;
    }
    let Some(destination) = packet::ip_destination(packet) else {
        traffic.drop_inner(
            peer_identity.as_deref(),
            zero_api::TrafficDropReason::InvalidPacket,
        );
        return;
    };
    let protocol = packet::ip_protocol(packet);
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
                if !pins.permits_peer(packet, &plane, peer_identity.clone()) {
                    continue;
                }
                let observer = pins.inner_io(packet, &plane, peer_identity.clone(), || {
                    route.outbound_inner_io(&tag)
                });
                match operation
                    .forward(
                        packet.to_vec(),
                        ingress_id,
                        responses.clone(),
                        generation,
                        observer.clone(),
                    )
                    .await
                {
                    Ok(observed) => {
                        pins.record_observed_peers(
                            packet,
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
                    Err(error) => tracing::debug!(%error, "packet route candidate unavailable"),
                }
            }
            PacketRouteTarget::Flow => {
                if !pins.permits_peer(packet, &PacketPlane::Flow, peer_identity.clone()) {
                    continue;
                }
                pins.record_peers(packet, PacketPlane::Flow, peer_identity.clone(), None);
                match protocol {
                    Some(packet::IPPROTO_TCP) => {
                        tcp.feed_with_peer(packet, peer_identity.clone()).await
                    }
                    Some(packet::IPPROTO_UDP) => {
                        udp.feed_with_peer(packet, peer_identity.clone()).await
                    }
                    _ => {}
                }
                return;
            }
            PacketRouteTarget::DirectEcho => {
                if !IcmpEchoRelay::accepts_direct_echo(packet) {
                    break;
                }
                if !pins.permits_peer(packet, &PacketPlane::DirectEcho, peer_identity.clone()) {
                    continue;
                }
                if echo.dispatch_direct(packet, effective_mtu.min(u16::MAX as usize) as u16) {
                    pins.record_peers(packet, PacketPlane::DirectEcho, peer_identity.clone(), None);
                    return;
                }
                break;
            }
            PacketRouteTarget::Block => {
                traffic.drop_inner(
                    peer_identity.as_deref(),
                    zero_api::TrafficDropReason::PolicyRejected,
                );
                if let Some(response) = packet::build_icmp_response(packet, effective_mtu) {
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
    if let Some(response) = packet::build_icmp_response(packet, effective_mtu) {
        traffic.send_response(responses, response, peer_identity.as_deref());
    }
}
