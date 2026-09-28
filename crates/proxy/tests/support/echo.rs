use std::net::{IpAddr, Ipv6Addr};
use zero_stack::packet;

pub fn request(source: IpAddr, destination: IpAddr, id: u16, seq: u16, data: &[u8]) -> Vec<u8> {
    let mut message = vec![0; 8];
    message[0] = if source.is_ipv4() { 8 } else { 128 };
    message[4..6].copy_from_slice(&id.to_be_bytes());
    message[6..8].copy_from_slice(&seq.to_be_bytes());
    message.extend_from_slice(data);
    let value = match (source, destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => packet::checksum(&message),
        (IpAddr::V6(source), IpAddr::V6(destination)) => v6_checksum(source, destination, &message),
        _ => panic!("address family mismatch"),
    };
    message[2..4].copy_from_slice(&value.to_be_bytes());
    packet::build_icmp_echo_tunnel_probe(
        &packet::IcmpEchoRequest {
            source,
            destination,
            message: &message,
        },
        id,
        source,
        65_535,
    )
    .unwrap()
}

pub fn reply(probe: &[u8]) -> Vec<u8> {
    let request = packet::parse_icmp_echo_request(probe).unwrap();
    let mut message = request.message.to_vec();
    message[0] = if request.source.is_ipv4() { 0 } else { 129 };
    message[2..4].fill(0);
    let value = match (request.destination, request.source) {
        (IpAddr::V4(_), IpAddr::V4(_)) => packet::checksum(&message),
        (IpAddr::V6(source), IpAddr::V6(destination)) => v6_checksum(source, destination, &message),
        _ => unreachable!(),
    };
    message[2..4].copy_from_slice(&value.to_be_bytes());
    let id = u16::from_be_bytes([message[4], message[5]]);
    packet::build_icmp_echo_reply(&request, id, &message, request.source, 65_535).unwrap()
}

pub fn v6_checksum(source: Ipv6Addr, destination: Ipv6Addr, message: &[u8]) -> u16 {
    let mut pseudo = source.octets().to_vec();
    pseudo.extend(destination.octets());
    pseudo.extend((message.len() as u32).to_be_bytes());
    pseudo.extend([0, 0, 0, packet::IPPROTO_ICMPV6]);
    pseudo.extend(message);
    packet::checksum(&pseudo)
}
