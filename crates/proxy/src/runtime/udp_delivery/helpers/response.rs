use std::net::SocketAddr;

use zero_core::Address;

use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::udp_dispatch::UdpDispatch;
#[cfg(feature = "upstream-association-runtime")]
use crate::runtime::udp_flow::response::UpstreamUdpResponse;

use super::accounting::UdpInboundResponseAccounting;
#[cfg(feature = "upstream-association-runtime")]
use super::parts::UdpUpstreamResponseParts;
use super::parts::{UdpChainResponseParts, UdpDirectResponseParts};

fn address_from_socket_addr(addr: SocketAddr) -> Address {
    match zero_platform_tokio::socket_addr_to_ip(addr) {
        zero_traits::IpAddress::V4(ip) => Address::Ipv4(ip),
        zero_traits::IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}

fn udp_response_target_from_socket_addr(addr: SocketAddr) -> (Address, u16) {
    (address_from_socket_addr(addr), addr.port())
}

#[cfg(feature = "upstream-association-runtime")]
pub(crate) fn record_upstream_udp_response_received(
    services: &UdpRuntimeServices,
    dispatch: &mut UdpDispatch,
    timeout: std::time::Duration,
    response: UpstreamUdpResponse,
) -> UdpUpstreamResponseParts {
    services.record_udp_upstream_packet_received();
    dispatch.touch_upstream_idle(timeout);
    let (target, port, payload) = response.into_parts();
    let session_id = match dispatch.upstream_association_view() {
        Some(association) => {
            dispatch.upstream_response_session_id(association.outbound_tag, &target, port)
        }
        None => udp_response_session_id(dispatch, &target, port),
    };
    dispatch.confirm_passive_health(session_id);
    let rate_limiters = dispatch.rate_limiters_by_session_id(session_id);
    let accounting = UdpInboundResponseAccounting::record_received(
        services,
        session_id,
        payload.len(),
        rate_limiters,
    );
    UdpUpstreamResponseParts {
        target,
        port,
        payload,
        accounting,
    }
}

fn record_direct_udp_response_received(
    services: &UdpRuntimeServices,
    dispatch: &UdpDispatch,
    sender: SocketAddr,
    payload_len: usize,
) -> UdpInboundResponseAccounting {
    let session_id = dispatch.direct_response_session_id(sender);
    dispatch.confirm_passive_health(session_id);
    let rate_limiters = dispatch.rate_limiters_by_session_id(session_id);
    UdpInboundResponseAccounting::record_received(services, session_id, payload_len, rate_limiters)
}

pub(crate) fn record_direct_udp_response_parts<'payload>(
    services: &UdpRuntimeServices,
    dispatch: &UdpDispatch,
    sender: SocketAddr,
    payload: &'payload [u8],
) -> UdpDirectResponseParts<'payload> {
    let accounting = record_direct_udp_response_received(services, dispatch, sender, payload.len());
    let (target, port) = udp_response_target_from_socket_addr(sender);
    UdpDirectResponseParts {
        target,
        port,
        payload,
        accounting,
    }
}

fn record_chain_udp_response_received(
    services: &UdpRuntimeServices,
    dispatch: &UdpDispatch,
    session_id: Option<u64>,
    payload_len: usize,
) -> UdpInboundResponseAccounting {
    let rate_limiters = dispatch.rate_limiters_by_session_id(session_id);
    UdpInboundResponseAccounting::record_received(services, session_id, payload_len, rate_limiters)
}

pub(crate) fn record_chain_udp_response_parts(
    services: &UdpRuntimeServices,
    dispatch: &UdpDispatch,
    target: Address,
    port: u16,
    payload: Vec<u8>,
    session_id: Option<u64>,
) -> UdpChainResponseParts {
    dispatch.confirm_passive_health(session_id);
    let accounting =
        record_chain_udp_response_received(services, dispatch, session_id, payload.len());
    UdpChainResponseParts {
        target,
        port,
        payload,
        accounting,
    }
}

#[cfg(feature = "upstream-association-runtime")]
fn udp_response_session_id(dispatch: &UdpDispatch, target: &Address, port: u16) -> Option<u64> {
    dispatch.session_id_by_target(target, port, None)
}
