use super::{checksum, icmpv6_checksum, IcmpEchoRequest, IPPROTO_ICMP, IPPROTO_ICMPV6};
use std::net::IpAddr;

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
