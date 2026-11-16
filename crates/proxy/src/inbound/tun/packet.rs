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
    inner: &mut zero_traits::PacketBuffer,
    route: &InboundRouteRuntimeFactory,
    _ingress_id: u64,
    pins: &mut PacketSessionPins,
    responses: &zero_stack::packet_output::PacketSender,
    echo: &IcmpEchoRelay,
    mtu: usize,
    dns_hijack: bool,
    traffic: Option<&dyn zero_traits::IoObserver>,
) -> bool {
    if let Some(response) = packet::build_icmp_mtu_response(inner, mtu) {
        if let Some(traffic) = traffic {
            traffic.dropped_reason(zero_traits::PacketDropReason::FragmentRejected);
        }
        send_response(responses, response, traffic);
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
            PacketRouteTarget::Packet { .. } => {
                panic!("native Packet execution requires the actual Source fixture and original provider");
            }
            PacketRouteTarget::Flow => {
                if !pins.permits(inner, &PacketPlane::Flow) {
                    continue;
                }
                pins.record(inner, PacketPlane::Flow);
                return false;
            }
            PacketRouteTarget::DirectEcho { dial_policy } => {
                if dial_policy != zero_traits::DialPolicy::default() {
                    break;
                }
                if !IcmpEchoRelay::accepts_direct_echo(inner) {
                    break;
                }
                if !pins.permits(inner, &PacketPlane::DirectEcho) {
                    continue;
                }
                // This standalone routing fixture keeps its input available
                // for policy assertions. The actual submitted copy must still
                // be admitted by the fixture's real memory owner before work.
                let Ok(mut owned) = responses.allocate_transmit(inner.len()) else {
                    break;
                };
                owned.copy_from_slice(inner);
                echo.dispatch_direct_owned(owned, mtu.min(u16::MAX as usize) as u16);
                pins.record(inner, PacketPlane::DirectEcho);
                return true;
            }
            PacketRouteTarget::Block => {
                if let Some(traffic) = traffic {
                    traffic.dropped_reason(zero_traits::PacketDropReason::PolicyRejected);
                }
                if let Some(response) = packet::build_icmp_response(inner, mtu) {
                    send_response(responses, response, traffic);
                }
                return true;
            }
            PacketRouteTarget::Unsupported => break,
            PacketRouteTarget::Fallback(_) => unreachable!("fallback candidates are flat"),
        }
    }
    if let Some(traffic) = traffic {
        traffic.dropped_reason(zero_traits::PacketDropReason::NoRoute);
    }
    if let Some(response) = packet::build_icmp_response(inner, mtu) {
        send_response(responses, response, traffic);
    }
    true
}

fn send_response(
    responses: &zero_stack::packet_output::PacketSender,
    response: Vec<u8>,
    observer: Option<&dyn zero_traits::IoObserver>,
) {
    if let Err(error) = responses.try_send(response) {
        if let Some(observer) = observer {
            observer.dropped_reason(match error {
                mpsc::error::TrySendError::Full(_) => zero_traits::PacketDropReason::QueueFull,
                mpsc::error::TrySendError::Closed(_) => zero_traits::PacketDropReason::QueueClosed,
            });
        }
    }
}

#[cfg(test)]
mod tests;
