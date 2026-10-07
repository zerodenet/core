//! ICMP Time Exceeded for packets stopped by an L3 forwarding hop.

use std::net::IpAddr;

use super::icmpv6_checksum;
use crate::packet::{checksum, ip_source, IPPROTO_ICMP, IPPROTO_ICMPV6};

/// Local path-MTU error with an explicitly supplied router address. The
/// original quote is unchanged; only the outer source/checksums are rebuilt.
pub fn build_icmp_mtu_error_response(
    original: &[u8],
    router_source: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    if original.len() <= mtu || crate::packet::ipv4_fragmentation_allowed(original) {
        return None;
    }
    let mut response = super::build_icmp_response(original, mtu)?;
    match (router_source, ip_source(original)?) {
        (IpAddr::V4(source), IpAddr::V4(_)) => {
            response[12..16].copy_from_slice(&source.octets());
            response[10..12].fill(0);
            let sum = checksum(&response[..20]);
            response[10..12].copy_from_slice(&sum.to_be_bytes());
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) => {
            response[8..24].copy_from_slice(&source.octets());
            response[42..44].fill(0);
            let sum = icmpv6_checksum(source, destination, &response[40..]);
            response[42..44].copy_from_slice(&sum.to_be_bytes());
        }
        _ => return None,
    }
    Some(response)
}

pub fn build_icmp_time_exceeded_response(
    original: &[u8],
    router_source: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    let destination = ip_source(original)?;
    match (router_source, destination, original.first()? >> 4) {
        (IpAddr::V4(source), IpAddr::V4(destination), 4) => {
            let header_len = usize::from(original[0] & 0x0f) * 4;
            if original.len() < 20
                || header_len < 20
                || header_len > original.len()
                || checksum(&original[..header_len]) != 0
                || original[9] == IPPROTO_ICMP && is_v4_error(original)
            {
                return None;
            }
            let quote_len = original.len().min(mtu.saturating_sub(28));
            if quote_len < 20 {
                return None;
            }
            let total = 28usize.checked_add(quote_len)?;
            let mut response = vec![0_u8; total];
            response[0] = 0x45;
            response[2..4].copy_from_slice(&u16::try_from(total).ok()?.to_be_bytes());
            response[8] = 64;
            response[9] = IPPROTO_ICMP;
            response[12..16].copy_from_slice(&source.octets());
            response[16..20].copy_from_slice(&destination.octets());
            response[20] = 11;
            response[21] = 0;
            response[28..].copy_from_slice(&original[..quote_len]);
            let icmp_checksum = checksum(&response[20..]);
            response[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
            let ip_checksum = checksum(&response[..20]);
            response[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
            Some(response)
        }
        (IpAddr::V6(source), IpAddr::V6(destination), 6) => {
            if original.len() < 40 || original[6] == IPPROTO_ICMPV6 && is_v6_error(original) {
                return None;
            }
            let quote_len = original.len().min(mtu.saturating_sub(48));
            if quote_len < 40 {
                return None;
            }
            let payload_len = 8usize.checked_add(quote_len)?;
            let total = 40usize.checked_add(payload_len)?;
            let mut response = vec![0_u8; total];
            response[0] = 0x60;
            response[4..6].copy_from_slice(&u16::try_from(payload_len).ok()?.to_be_bytes());
            response[6] = IPPROTO_ICMPV6;
            response[7] = 64;
            response[8..24].copy_from_slice(&source.octets());
            response[24..40].copy_from_slice(&destination.octets());
            response[40] = 3;
            response[41] = 0;
            response[48..].copy_from_slice(&original[..quote_len]);
            let icmp_checksum = icmpv6_checksum(source, destination, &response[40..]);
            response[42..44].copy_from_slice(&icmp_checksum.to_be_bytes());
            Some(response)
        }
        _ => None,
    }
}

fn is_v4_error(packet: &[u8]) -> bool {
    let header_len = usize::from(packet[0] & 0x0f) * 4;
    packet
        .get(header_len)
        .is_some_and(|kind| matches!(kind, 3 | 4 | 5 | 11 | 12))
}

fn is_v6_error(packet: &[u8]) -> bool {
    packet.get(40).is_some_and(|kind| *kind < 128)
}
