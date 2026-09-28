//! Packet-plane route from an OS TUN ingress to a packet-capable outbound.

use crate::{
    inventory::PacketRouteTarget,
    runtime::{
        inbound_operation::raw_ip::IcmpEchoRelay,
        packet_route::{PacketPlane, PacketSessionPins},
        route_runtime::InboundRouteRuntimeFactory,
    },
};
use tokio::sync::mpsc;
use zero_stack::packet;

#[allow(clippy::too_many_arguments)]
pub(super) async fn try_forward(
    inner: &[u8],
    route: &InboundRouteRuntimeFactory,
    ingress_id: u64,
    pins: &mut PacketSessionPins,
    responses: &mpsc::Sender<Vec<u8>>,
    echo: &IcmpEchoRelay,
    mtu: usize,
    dns_hijack: bool,
) -> bool {
    if let Some(response) = packet::build_icmp_mtu_response(inner, mtu) {
        let _ = responses.try_send(response);
        return true;
    }
    if dns_hijack
        && (packet::parse_tcp(inner).is_some_and(|tcp| tcp.dst.port == 53)
            || packet::parse_udp(inner).is_some_and(|udp| udp.dst.port == 53))
    {
        return false;
    }
    let Some(destination) = packet::ip_destination(inner) else {
        return false;
    };
    for candidate in route
        .packet_route_target(destination, packet::ip_protocol(inner))
        .into_candidates()
    {
        match candidate {
            PacketRouteTarget::Packet {
                tag,
                translated,
                operation,
            } => {
                let generation = route.icmp_egress_generation();
                let plane = if translated {
                    PacketPlane::TranslatedPacket(tag)
                } else {
                    PacketPlane::Packet(tag)
                };
                if !pins.permits(inner, &plane) {
                    continue;
                }
                match operation
                    .forward(inner.to_vec(), ingress_id, responses.clone(), generation)
                    .await
                {
                    Ok(Some(response)) => {
                        pins.record(inner, plane);
                        let _ = responses.try_send(response);
                        return true;
                    }
                    Ok(None) => {
                        pins.record(inner, plane);
                        return true;
                    }
                    Err(error) => tracing::debug!(%error, "TUN packet route candidate unavailable"),
                }
            }
            PacketRouteTarget::Flow => {
                if !pins.permits(inner, &PacketPlane::Flow) {
                    continue;
                }
                pins.record(inner, PacketPlane::Flow);
                return false;
            }
            PacketRouteTarget::DirectEcho => {
                if !IcmpEchoRelay::accepts_direct_echo(inner) {
                    break;
                }
                if !pins.permits(inner, &PacketPlane::DirectEcho) {
                    continue;
                }
                if echo.dispatch_direct(inner, mtu.min(u16::MAX as usize) as u16) {
                    pins.record(inner, PacketPlane::DirectEcho);
                    return true;
                }
                break;
            }
            PacketRouteTarget::Block | PacketRouteTarget::Unsupported => break,
            PacketRouteTarget::Fallback(_) => unreachable!("fallback candidates are flat"),
        }
    }
    if let Some(response) = packet::build_icmp_response(inner, mtu) {
        let _ = responses.try_send(response);
    }
    true
}
