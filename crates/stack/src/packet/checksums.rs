//! Checksums borrow transport payloads instead of allocating a joined packet.
use std::net::{Ipv4Addr, Ipv6Addr};

use super::{IPPROTO_TCP, IPPROTO_UDP};

fn word_sum(data: &[u8]) -> u64 {
    let (words, remainder) = data.as_chunks::<2>();
    let sum = words
        .iter()
        .map(|pair| u64::from(u16::from_be_bytes([pair[0], pair[1]])))
        .sum::<u64>();
    sum + remainder.first().map_or(0, |byte| u64::from(*byte) << 8)
}

fn finish(mut sum: u64) -> u16 {
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// RFC 1071 ones' complement checksum. Serialize the result in network order.
pub fn checksum(data: &[u8]) -> u16 {
    finish(word_sum(data))
}

fn transport_checksum_v4(src: &Ipv4Addr, dst: &Ipv4Addr, protocol: u8, data: &[u8]) -> u16 {
    let mut header = [0_u8; 12];
    header[..4].copy_from_slice(&src.octets());
    header[4..8].copy_from_slice(&dst.octets());
    header[9] = protocol;
    header[10..].copy_from_slice(&(data.len() as u16).to_be_bytes());
    // The pseudo-header is even-sized, so only the payload can need padding.
    finish(word_sum(&header) + word_sum(data))
}

pub(super) fn transport_checksum_v6(
    src: &Ipv6Addr,
    dst: &Ipv6Addr,
    protocol: u8,
    data: &[u8],
) -> u16 {
    let mut header = [0_u8; 40];
    header[..16].copy_from_slice(&src.octets());
    header[16..32].copy_from_slice(&dst.octets());
    header[32..36].copy_from_slice(&(data.len() as u32).to_be_bytes());
    header[39] = protocol;
    finish(word_sum(&header) + word_sum(data))
}

pub(super) fn tcp_checksum_v4(src: &Ipv4Addr, dst: &Ipv4Addr, tcp: &[u8]) -> u16 {
    transport_checksum_v4(src, dst, IPPROTO_TCP, tcp)
}

pub(super) fn tcp_checksum_v6(src: &Ipv6Addr, dst: &Ipv6Addr, tcp: &[u8]) -> u16 {
    transport_checksum_v6(src, dst, IPPROTO_TCP, tcp)
}

fn udp_wire_value(checksum: u16) -> u16 {
    // RFC 768 encodes a computed zero checksum as all ones.
    if checksum == 0 {
        0xffff
    } else {
        checksum
    }
}

pub(super) fn udp_checksum_v4(src: &Ipv4Addr, dst: &Ipv4Addr, udp: &[u8]) -> u16 {
    udp_wire_value(transport_checksum_v4(src, dst, IPPROTO_UDP, udp))
}

pub(super) fn udp_checksum_v6(src: &Ipv6Addr, dst: &Ipv6Addr, udp: &[u8]) -> u16 {
    udp_wire_value(transport_checksum_v6(src, dst, IPPROTO_UDP, udp))
}

#[cfg(test)]
mod tests;
