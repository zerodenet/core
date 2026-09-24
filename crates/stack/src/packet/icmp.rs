use std::net::{IpAddr, Ipv6Addr};

use super::{checksum, transport_header, IPPROTO_ICMP, IPPROTO_ICMPV6, IPPROTO_TCP, IPPROTO_UDP};

mod error;
mod forward;
pub use error::{parse_icmp_error, IcmpErrorKind, ParsedIcmpError};
pub use forward::build_icmp_time_exceeded_response;

pub struct IcmpEchoRequest<'a> {
    pub source: IpAddr,
    pub destination: IpAddr,
    pub message: &'a [u8],
}

pub struct IcmpEchoReply<'a> {
    pub source: IpAddr,
    pub destination: IpAddr,
    pub identifier: u16,
    pub message: &'a [u8],
}

pub fn parse_icmp_echo_reply(packet: &[u8]) -> Option<IcmpEchoReply<'_>> {
    let transport = transport_header(packet)?;
    let message = packet.get(transport.offset..transport.end)?;
    if message.len() < 8 || message[1] != 0 {
        return None;
    }
    match (transport.src, transport.dst, transport.protocol, message[0]) {
        (IpAddr::V4(_), IpAddr::V4(_), IPPROTO_ICMP, 0) if checksum(message) == 0 => {}
        (IpAddr::V6(source), IpAddr::V6(destination), IPPROTO_ICMPV6, 129)
            if icmpv6_checksum(source, destination, message) == 0 => {}
        _ => return None,
    }
    Some(IcmpEchoReply {
        source: transport.src,
        destination: transport.dst,
        identifier: u16::from_be_bytes([message[4], message[5]]),
        message,
    })
}

pub fn parse_icmp_echo_request(packet: &[u8]) -> Option<IcmpEchoRequest<'_>> {
    let transport = transport_header(packet)?;
    let message = packet.get(transport.offset..transport.end)?;
    if message.len() < 8 || message[1] != 0 {
        return None;
    }
    match (transport.src, transport.dst, transport.protocol, message[0]) {
        (IpAddr::V4(_), IpAddr::V4(_), IPPROTO_ICMP, 8) if checksum(message) == 0 => {}
        (IpAddr::V6(source), IpAddr::V6(destination), IPPROTO_ICMPV6, 128)
            if icmpv6_checksum(source, destination, message) == 0 => {}
        _ => return None,
    }
    Some(IcmpEchoRequest {
        source: transport.src,
        destination: transport.dst,
        message,
    })
}

pub fn build_icmp_echo_probe(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    local: IpAddr,
) -> Option<Vec<u8>> {
    let mut probe = request.message.to_vec();
    probe[4..6].copy_from_slice(&probe_id.to_be_bytes());
    probe[2..4].fill(0);
    let checksum = match (local, request.destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => checksum(&probe),
        (IpAddr::V6(local), IpAddr::V6(destination)) => icmpv6_checksum(local, destination, &probe),
        _ => return None,
    };
    probe[2..4].copy_from_slice(&checksum.to_be_bytes());
    Some(probe)
}

/// Build one inner IP echo request for a packet-capable outbound. The source
/// is the outbound tunnel address; the original inbound source stays private.
pub fn build_icmp_echo_tunnel_probe(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    local: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    let message = build_icmp_echo_probe(request, probe_id, local)?;
    let header_size: usize = match (local, request.destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => 20,
        (IpAddr::V6(_), IpAddr::V6(_)) => 40,
        _ => return None,
    };
    let total = header_size.checked_add(message.len())?;
    if total > mtu || total > u16::MAX as usize {
        return None;
    }
    let mut packet = vec![0_u8; total];
    match (local, request.destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            packet[0] = 0x45;
            packet[2..4].copy_from_slice(&(total as u16).to_be_bytes());
            packet[8] = 64;
            packet[9] = IPPROTO_ICMP;
            packet[12..16].copy_from_slice(&source.octets());
            packet[16..20].copy_from_slice(&destination.octets());
            let ip_checksum = checksum(&packet[..20]);
            packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            packet[0] = 0x60;
            packet[4..6].copy_from_slice(&(message.len() as u16).to_be_bytes());
            packet[6] = IPPROTO_ICMPV6;
            packet[7] = 64;
            packet[8..24].copy_from_slice(&source.octets());
            packet[24..40].copy_from_slice(&destination.octets());
        }
        _ => return None,
    }
    packet[header_size..].copy_from_slice(&message);
    Some(packet)
}

pub fn build_icmp_echo_reply(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    reply: &[u8],
    local: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    if reply.len() < 8
        || reply[1] != 0
        || reply[4..6] != probe_id.to_be_bytes()
        || reply[6..8] != request.message[6..8]
        || reply[8..] != request.message[8..]
    {
        return None;
    }
    let mut message = reply.to_vec();
    let header_size: usize = match (request.source, request.destination, local, reply[0]) {
        (IpAddr::V4(_), IpAddr::V4(_), IpAddr::V4(_), 0) if checksum(reply) == 0 => 20,
        (IpAddr::V6(_), IpAddr::V6(destination), IpAddr::V6(local), 129)
            if icmpv6_checksum(destination, local, reply) == 0 =>
        {
            40
        }
        _ => return None,
    };
    let total = header_size.checked_add(message.len())?;
    if total > mtu || total > u16::MAX as usize {
        return None;
    }
    message[4..6].copy_from_slice(&request.message[4..6]);
    message[2..4].fill(0);
    let mut response = vec![0_u8; total];
    match (request.destination, request.source) {
        (IpAddr::V4(source), IpAddr::V4(destination)) => {
            let icmp_checksum = checksum(&message);
            message[2..4].copy_from_slice(&icmp_checksum.to_be_bytes());
            response[0] = 0x45;
            response[2..4].copy_from_slice(&(total as u16).to_be_bytes());
            response[8] = 64;
            response[9] = IPPROTO_ICMP;
            response[12..16].copy_from_slice(&source.octets());
            response[16..20].copy_from_slice(&destination.octets());
            let ip_checksum = checksum(&response[..20]);
            response[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            let icmp_checksum = icmpv6_checksum(source, destination, &message);
            message[2..4].copy_from_slice(&icmp_checksum.to_be_bytes());
            response[0] = 0x60;
            response[4..6].copy_from_slice(&(message.len() as u16).to_be_bytes());
            response[6] = IPPROTO_ICMPV6;
            response[7] = 64;
            response[8..24].copy_from_slice(&source.octets());
            response[24..40].copy_from_slice(&destination.octets());
        }
        _ => return None,
    }
    response[header_size..].copy_from_slice(&message);
    Some(response)
}

pub fn build_icmp_echo_unreachable_response(packet: &[u8], mtu: usize) -> Option<Vec<u8>> {
    parse_icmp_echo_request(packet)?;
    match packet[0] >> 4 {
        4 => build_ipv4_response(packet, mtu, true),
        6 => build_ipv6_response(packet, mtu, true),
        _ => None,
    }
}

/// Return only MTU errors for echo requests so a caller with an ICMP relay
/// can forward them. Other traffic keeps the ordinary rejection policy.
pub fn build_icmp_mtu_response(packet: &[u8], mtu: usize) -> Option<Vec<u8>> {
    if packet.len() <= mtu && parse_icmp_echo_request(packet).is_some() {
        return None;
    }
    build_icmp_response(packet, mtu)
}

/// Build an explicit ICMP response for traffic the TUN stack cannot carry.
/// TCP/UDP within the configured MTU return `None` and continue normally.
pub fn build_icmp_response(packet: &[u8], mtu: usize) -> Option<Vec<u8>> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => build_ipv4_response(packet, mtu, false),
        Some(6) => build_ipv6_response(packet, mtu, false),
        _ => None,
    }
}

/// Build an administrative-prohibited response for a UDP datagram rejected
/// by the local TUN policy. The response remains bounded to the configured
/// MTU and quotes the original IP/UDP headers for kernel error correlation.
pub fn build_udp_unreachable_response(packet: &[u8], mtu: usize) -> Option<Vec<u8>> {
    if super::transport_header(packet)?.protocol != IPPROTO_UDP {
        return None;
    }
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => build_ipv4_response(packet, mtu, true),
        Some(6) => build_ipv6_response(packet, mtu, true),
        _ => None,
    }
}

fn build_ipv4_response(packet: &[u8], mtu: usize, reject_udp: bool) -> Option<Vec<u8>> {
    let transport = transport_header(packet)?;
    let oversized = packet.len() > mtu;
    if !oversized && matches!(transport.protocol, IPPROTO_TCP | IPPROTO_UDP) && !reject_udp {
        return None;
    }
    if transport.protocol == IPPROTO_ICMP
        && packet
            .get(transport.offset)
            .copied()
            .is_none_or(|kind| kind != 8)
    {
        return None;
    }

    let source = match transport.dst {
        std::net::IpAddr::V4(address) => address,
        _ => return None,
    };
    let destination = match transport.src {
        std::net::IpAddr::V4(address) => address,
        _ => return None,
    };
    let maximum_quote = mtu.saturating_sub(28);
    let quote_length = packet
        .len()
        .min(maximum_quote)
        .max(transport.offset.min(packet.len()));
    let icmp_length = 8usize.checked_add(quote_length)?;
    let total_length = 20usize.checked_add(icmp_length)?;
    let total_length_u16 = u16::try_from(total_length).ok()?;
    let mut response = vec![0_u8; total_length];
    response[0] = 0x45;
    response[2..4].copy_from_slice(&total_length_u16.to_be_bytes());
    response[8] = 64;
    response[9] = IPPROTO_ICMP;
    response[12..16].copy_from_slice(&source.octets());
    response[16..20].copy_from_slice(&destination.octets());

    let offset = 20;
    response[offset] = 3;
    response[offset + 1] = if oversized { 4 } else { 13 };
    if oversized {
        response[offset + 6..offset + 8]
            .copy_from_slice(&(mtu.min(u16::MAX as usize) as u16).to_be_bytes());
    }
    response[offset + 8..].copy_from_slice(&packet[..quote_length]);
    let icmp_checksum = checksum(&response[offset..]);
    response[offset + 2..offset + 4].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = checksum(&response[..20]);
    response[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    Some(response)
}

fn build_ipv6_response(packet: &[u8], mtu: usize, reject_udp: bool) -> Option<Vec<u8>> {
    let transport = transport_header(packet)?;
    let oversized = packet.len() > mtu;
    if !oversized && matches!(transport.protocol, IPPROTO_TCP | IPPROTO_UDP) && !reject_udp {
        return None;
    }
    if transport.protocol == IPPROTO_ICMPV6
        && packet
            .get(transport.offset)
            .copied()
            .is_none_or(|kind| kind != 128)
    {
        return None;
    }

    let source = match transport.dst {
        std::net::IpAddr::V6(address) => address,
        _ => return None,
    };
    let destination = match transport.src {
        std::net::IpAddr::V6(address) => address,
        _ => return None,
    };
    let quote_length = packet.len().min(mtu.saturating_sub(48));
    let icmp_length = 8usize.checked_add(quote_length)?;
    let icmp_length_u16 = u16::try_from(icmp_length).ok()?;
    let mut response = vec![0_u8; 40 + icmp_length];
    response[0] = 0x60;
    response[4..6].copy_from_slice(&icmp_length_u16.to_be_bytes());
    response[6] = IPPROTO_ICMPV6;
    response[7] = 64;
    response[8..24].copy_from_slice(&source.octets());
    response[24..40].copy_from_slice(&destination.octets());

    let offset = 40;
    response[offset] = if oversized { 2 } else { 1 };
    response[offset + 1] = if oversized { 0 } else { 1 };
    if oversized {
        response[offset + 4..offset + 8]
            .copy_from_slice(&(mtu.min(u32::MAX as usize) as u32).to_be_bytes());
    }
    response[offset + 8..].copy_from_slice(&packet[..quote_length]);
    let icmp_checksum = icmpv6_checksum(source, destination, &response[offset..]);
    response[offset + 2..offset + 4].copy_from_slice(&icmp_checksum.to_be_bytes());
    Some(response)
}

fn icmpv6_checksum(source: Ipv6Addr, destination: Ipv6Addr, icmp: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(40 + icmp.len());
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(icmp.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, IPPROTO_ICMPV6]);
    pseudo.extend_from_slice(icmp);
    checksum(&pseudo)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn rejected_udp_builds_bounded_ipv4_and_ipv6_errors() {
        let ipv4 = crate::packet::build_udp(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)),
            50_000,
            443,
            b"rejected-v4",
        );
        let response = build_udp_unreachable_response(&ipv4, 1500).unwrap();
        assert_eq!(response[9], IPPROTO_ICMP);
        assert_eq!(&response[12..16], &[203, 0, 113, 7]);
        assert_eq!(&response[16..20], &[10, 0, 0, 2]);
        assert_eq!(&response[20..22], &[3, 13]);
        assert!(response.len() <= 1500);

        let ipv6 = crate::packet::build_udp(
            IpAddr::V6("fd00::2".parse::<Ipv6Addr>().unwrap()),
            IpAddr::V6("2001:db8::7".parse::<Ipv6Addr>().unwrap()),
            50_001,
            443,
            b"rejected-v6",
        );
        let response = build_udp_unreachable_response(&ipv6, 1500).unwrap();
        assert_eq!(response[6], IPPROTO_ICMPV6);
        assert_eq!(&response[40..42], &[1, 1]);
        assert!(response.len() <= 1500);
    }
}
