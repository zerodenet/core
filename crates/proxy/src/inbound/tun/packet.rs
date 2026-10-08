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
    inner: &mut Vec<u8>,
    route: &InboundRouteRuntimeFactory,
    ingress_id: u64,
    pins: &mut PacketSessionPins,
    responses: &mpsc::Sender<Vec<u8>>,
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
    let packet_key = packet::packet_conversation_key(inner);
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
                    PacketPlane::TranslatedPacket(tag.clone())
                } else {
                    PacketPlane::Packet(tag.clone())
                };
                if !pins.permits(inner, &plane) {
                    continue;
                }
                let observer = pins.inner_io(inner, &plane, None, || route.outbound_inner_io(&tag));
                let Some(replies) = pins.replies_for(inner, &plane, None, responses.clone()) else {
                    continue;
                };
                match operation
                    .forward(inner, ingress_id, replies, generation, observer.clone())
                    .await
                {
                    Ok(observed) => {
                        pins.record_observed_key(
                            packet_key,
                            plane,
                            None,
                            observed.peer_identity,
                            observer,
                        );
                        if let Some(response) = observed.response {
                            send_response(responses, response, traffic);
                        }
                        return true;
                    }
                    Err(error) => {
                        pins.reject_unaccepted_key(packet_key, None);
                        if inner.is_empty() {
                            break;
                        }
                        tracing::debug!(%error, "TUN packet route candidate unavailable");
                    }
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
    responses: &mpsc::Sender<Vec<u8>>,
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
