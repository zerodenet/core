//! Routing for decrypted raw IP packets.

use std::time::Instant;
use tokio::sync::mpsc;
use zero_stack::{packet, FragmentOutcome, FragmentReassembler, UserTcpStack, UserUdpStack};
use zero_traits::{TcpStack, UdpStack};

use super::IcmpEchoRelay;
use crate::inventory::PacketRouteTarget;
use crate::runtime::packet_route::{PacketPlane, PacketSessionPins};

#[allow(clippy::too_many_arguments)]
pub(super) async fn feed_inner_packet(
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
