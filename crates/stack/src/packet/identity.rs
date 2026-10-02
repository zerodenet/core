//! Pure packet conversation metadata consumed by route pinning.

use super::{
    ip_destination, ip_protocol, ip_source, parse_icmp_echo_reply, parse_icmp_echo_request,
    parse_tcp, parse_udp, IPPROTO_ICMP, IPPROTO_ICMPV6, IPPROTO_TCP, IPPROTO_UDP,
};
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PacketConversationKey {
    pub source: IpAddr,
    pub destination: IpAddr,
    pub protocol: u8,
    pub transport_identity: Option<(u16, u16)>,
}

pub fn packet_conversation_key(packet: &[u8]) -> Option<PacketConversationKey> {
    let protocol = ip_protocol(packet)?;
    let transport_identity = match protocol {
        IPPROTO_TCP => {
            let tcp = parse_tcp(packet)?;
            Some((tcp.src.port, tcp.dst.port))
        }
        IPPROTO_UDP => {
            let udp = parse_udp(packet)?;
            Some((udp.src.port, udp.dst.port))
        }
        IPPROTO_ICMP | IPPROTO_ICMPV6 => parse_icmp_echo_request(packet)
            .map(|echo| (u16::from_be_bytes([echo.message[4], echo.message[5]]), 0))
            .or_else(|| parse_icmp_echo_reply(packet).map(|echo| (echo.identifier, 0))),
        _ => None,
    };
    Some(PacketConversationKey {
        source: ip_source(packet)?,
        destination: ip_destination(packet)?,
        protocol,
        transport_identity,
    })
}

/// Correlate a reply (or an ICMP quote of the sent packet) to its outbound
/// conversation. This is pure metadata; it does not choose an execution role.
pub fn packet_return_key(packet: &[u8]) -> Option<PacketConversationKey> {
    if let Some(error) = super::parse_icmp_error(packet) {
        return Some(PacketConversationKey {
            source: error.quoted_source.ip,
            destination: error.quoted_destination.ip,
            protocol: error.quoted_protocol,
            transport_identity: Some((error.quoted_source.port, error.quoted_destination.port)),
        });
    }
    let mut key = packet_conversation_key(packet)?;
    std::mem::swap(&mut key.source, &mut key.destination);
    if matches!(key.protocol, IPPROTO_TCP | IPPROTO_UDP) {
        key.transport_identity = key
            .transport_identity
            .map(|(source, destination)| (destination, source));
    }
    Some(key)
}
